#!/usr/bin/env bash
# DGX Spark Controller - system update helper for one DGX Spark node.
#
# The controller installs this file root-owned at
# /usr/local/sbin/dgx-spark-controller-update with `sudo bash <copy> install`,
# which also adds a sudoers rule that lets the installing user run exactly
# these subcommands without a password:
#
#   check     refresh package lists and firmware metadata, count updates
#   download  fetch every pending package and firmware file, install nothing
#   apply     install pending packages and firmware
#   status    report the state of the last download or apply run
#   reboot    reboot the node a few seconds after this command returns
#
# The node needs no Internet access. Every download goes through the
# controller's update relay, which the controller forwards to 127.0.0.1:18877
# on this node over its SSH session while it runs check, download or apply.
# The relay serves files from the Internet or from a directory on the
# controller; apt and fwupd verify their signatures here, on the node.
#
# The controller verifies this file's SHA-256 before every use. The helper
# takes no input beyond its single subcommand: no other argument, no
# environment and no file.
set -euo pipefail
umask 022
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
export LC_ALL=C

readonly HELPER=/usr/local/sbin/dgx-spark-controller-update
readonly SUDOERS=/etc/sudoers.d/dgx-spark-controller-update
readonly STATE_DIR=/var/lib/dgx-spark-controller-update
readonly STATE_FILE=$STATE_DIR/state
readonly APT_DIR=$STATE_DIR/apt
readonly FIRMWARE_DIR=$STATE_DIR/firmware
readonly LOG_FILE=/var/log/dgx-spark-controller-update.log
readonly UNIT=dgx-spark-controller-update
readonly REBOOT_UNIT=dgx-spark-controller-reboot
readonly HELPER_VERSION=1
readonly RELAY=127.0.0.1:18877
readonly RELAY_MODE_URL=http://dgx-spark-controller.invalid/mode
readonly LVFS_METADATA_URL=http://cdn.fwupd.org/downloads/firmware.xml.zst
readonly LVFS_REMOTE=lvfs
# Upgrades of these packages take effect only after a reboot even when the
# package does not request one: kernels, kernel modules, the GPU driver and
# firmware.
readonly REBOOT_PACKAGES='^(linux-|nvidia-|libnvidia-|cuda-|libcuda|.*firmware)'

fail() {
  printf 'ERROR=%s\n' "$1"
  exit "${2:-1}"
}

require_root() {
  [ "$(id -u)" -eq 0 ] || fail not-root 77
}

boot_id() {
  cat /proc/sys/kernel/random/boot_id
}

count_lines() {
  if [ -z "$1" ]; then echo 0; else printf '%s\n' "$1" | wc -l; fi
}

unit_active() {
  systemctl is-active --quiet "$UNIT.service"
}

# Download one URL through the relay to a file, atomically.
relay_get() {
  local tmp
  tmp=$(mktemp "$2.XXXXXX")
  if curl -fsS --retry 2 --connect-timeout 10 --max-time 3600 \
    --max-filesize 1073741824 --proxy "http://$RELAY" -o "$tmp" "$1"; then
    mv -f -- "$tmp" "$2"
  else
    rm -f -- "$tmp"
    return 1
  fi
}

# Print the relay's source mode ("online" or "directory"); fail without one.
relay_mode() {
  local mode
  mode=$(curl -fsS --connect-timeout 5 --max-time 15 --proxy "http://$RELAY" \
    "$RELAY_MODE_URL" 2>/dev/null) || return 1
  case "$mode" in online | directory) printf '%s\n' "$mode" ;; *) return 1 ;; esac
}

# Write an apt configuration that reaches every repository through the relay.
# Signed sources are rewritten to http:// so the relay can serve them from its
# cache or directory; apt still verifies the signed indexes and packages.
# Sources that need credentials (auth.conf, for example Ubuntu Pro) or that
# apt trusts without a signature keep their URLs, so TLS stays end to end
# through the relay's tunnel in online mode; directory mode leaves them out.
prepare_apt() {
  local mode=$1
  rm -rf -- "$APT_DIR" || return 1
  mkdir -p "$APT_DIR/sources.list.d" || return 1
  python3 -I - "$mode" "$APT_DIR" <<'PY' || return 1
import glob, os, re, sys

mode, target = sys.argv[1], sys.argv[2]
auth_hosts = set()
for path in ["/etc/apt/auth.conf", *sorted(glob.glob("/etc/apt/auth.conf.d/*"))]:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            words = handle.read().split()
    except OSError:
        continue
    for index, word in enumerate(words[:-1]):
        if word == "machine":
            host = re.sub(r"^[a-z+]+://", "", words[index + 1]).split("/")[0]
            auth_hosts.add(host.split(":")[0].lower())

uri_re = re.compile(r"\b(https?)://([A-Za-z0-9.-]+)")
# One-line [trusted=yes] and deb822 "Trusted: yes", with any value apt reads
# as true.
unsigned_re = re.compile(
    r"(?i)\b(trusted|allow-insecure|allow-weak|allow-downgrade-to-insecure)"
    r"\s*[=:]\s*(yes|true|with|on|enable|1)\b"
)

def rewrite(text):
    hosts = {match.group(2).lower() for match in uri_re.finditer(text)}
    if hosts & auth_hosts or unsigned_re.search(text):
        return text if mode == "online" else None
    return uri_re.sub(lambda match: "http://" + match.group(2), text)

sources = [("/etc/apt/sources.list", os.path.join(target, "sources.list"))]
for path in sorted(glob.glob("/etc/apt/sources.list.d/*")):
    name = os.path.basename(path)
    if name.endswith((".list", ".sources")):
        sources.append((path, os.path.join(target, "sources.list.d", name)))
for source, destination in sources:
    try:
        with open(source, encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        text = ""
    rewritten = rewrite(text)
    if rewritten is None and destination.endswith("sources.list"):
        rewritten = ""
    if rewritten is not None:
        with open(destination, "w", encoding="utf-8") as handle:
            handle.write(rewritten)
PY
  cat >"$APT_DIR/apt.conf" <<EOF || return 1
Acquire::http::Proxy "http://$RELAY/";
Acquire::https::Proxy "http://$RELAY/";
Acquire::Retries "3";
Acquire::Languages "none";
Acquire::PDiffs "false";
Acquire::By-Hash "no";
Acquire::AllowInsecureRepositories "false";
Acquire::AllowDowngradeToInsecureRepositories "false";
Dir::Etc::SourceList "$APT_DIR/sources.list";
Dir::Etc::SourceParts "$APT_DIR/sources.list.d";
DPkg::Lock::Timeout "1200";
EOF
}

apt_relay() {
  apt-get -c "$APT_DIR/apt.conf" "$@"
}

# Package names a full upgrade would install, from the current package lists.
pending_packages() {
  apt_relay -s -o Debug::NoLocking=1 full-upgrade 2>/dev/null \
    | awk '$1 == "Inst" { print $2 }' || true
}

# Load the LVFS metadata through the relay; fwupd verifies its signature.
# Returns 2 when the relay could not provide the files, 1 when fwupd refused.
firmware_refresh() {
  command -v fwupdmgr >/dev/null 2>&1 || return 0
  mkdir -p "$FIRMWARE_DIR"
  relay_get "$LVFS_METADATA_URL" "$FIRMWARE_DIR/firmware.xml.zst" || return 2
  relay_get "$LVFS_METADATA_URL.jcat" "$FIRMWARE_DIR/firmware.xml.zst.jcat" || return 2
  local status=0
  fwupdmgr refresh "$FIRMWARE_DIR/firmware.xml.zst" \
    "$FIRMWARE_DIR/firmware.xml.zst.jcat" "$LVFS_REMOTE" >/dev/null 2>&1 || status=$?
  # Exit status 2: fwupd already holds this signed metadata or a newer one.
  [ "$status" -eq 0 ] || [ "$status" -eq 2 ] || return 1
}

# One "<device-id> <sha256> <url>" line per device with an applicable release.
pending_firmware_releases() {
  command -v fwupdmgr >/dev/null 2>&1 || return 0
  local json
  json=$(fwupdmgr get-updates --json --no-unreported-check --no-metadata-check \
    2>/dev/null) || true
  printf '%s' "$json" | python3 -I -c '
import json, re, sys
try:
    devices = json.load(sys.stdin).get("Devices", [])
except (ValueError, AttributeError):
    devices = []
for device in devices:
    releases = device.get("Releases") if isinstance(device, dict) else None
    if not releases:
        continue
    release = releases[0]
    device_id = str(device.get("DeviceId", ""))
    digests = [str(item) for item in release.get("Checksum", [])]
    sha256 = next((item for item in digests if re.fullmatch(r"[0-9a-f]{64}", item)), "")
    urls = [str(item) for item in release.get("Locations", [])]
    url = next((item for item in urls if re.fullmatch(r"https?://[A-Za-z0-9.-]+/[A-Za-z0-9._/~+-]+\.cab", item)), "")
    if re.fullmatch(r"[0-9a-f]{40}", device_id) and sha256 and url:
        print(device_id, sha256, re.sub(r"^https://", "http://", url))
' || true
}

# Download every pending firmware cabinet once; the file name is its SHA-256.
firmware_download() {
  local device_id sha256 url file
  while read -r device_id sha256 url; do
    [ -n "$url" ] || continue
    file=$FIRMWARE_DIR/$sha256.cab
    if [ -f "$file" ] && [ "$(sha256sum "$file" | cut -d' ' -f1)" = "$sha256" ]; then
      continue
    fi
    relay_get "$url" "$file" || return 1
    [ "$(sha256sum "$file" | cut -d' ' -f1)" = "$sha256" ] || return 1
  done < <(pending_firmware_releases)
}

firmware_install() {
  local device_id sha256 url file status
  while read -r device_id sha256 url; do
    [ -n "$url" ] || continue
    file=$FIRMWARE_DIR/$sha256.cab
    [ "$(sha256sum "$file" | cut -d' ' -f1)" = "$sha256" ] || return 1
    status=0
    fwupdmgr local-install --assume-yes --no-reboot-check "$file" "$device_id" \
      || status=$?
    # Exit status 2 means fwupd had nothing left to do for the device.
    if [ "$status" -ne 0 ] && [ "$status" -ne 2 ]; then return 1; fi
  done < <(pending_firmware_releases)
}

reboot_requested() {
  if [ -e /run/reboot-required ]; then echo 1; else echo 0; fi
}

# A directory source may hold no LVFS metadata; firmware is then skipped.
FIRMWARE_SOURCE=1

# Firmware metadata through the relay, or fail; a directory may lack it,
# but metadata that fwupd refuses always fails.
refresh_firmware_or_skip() {
  local status=0
  firmware_refresh || status=$?
  [ "$status" -eq 0 ] && return 0
  [ "$status" -eq 2 ] && [ "$1" = directory ] || return 1
  FIRMWARE_SOURCE=0
  printf '=== no LVFS metadata in the update directory; skipping firmware\n'
}

firmware_count() {
  if [ "$FIRMWARE_SOURCE" = 1 ]; then
    count_lines "$(pending_firmware_releases)"
  else
    echo 0
  fi
}

# Every run state is kept in these variables and written as a whole.
STATE=idle
MODE=
STEP=
STARTED_AT=
FINISHED_AT=
RUN_BOOT_ID=
PACKAGES=
FIRMWARE=
REBOOT_REQUIRED=
FAILED_STEP=

load_state() {
  [ -r "$STATE_FILE" ] || return 0
  local line key value
  while IFS= read -r line || [ -n "$line" ]; do
    key=${line%%=*}
    value=${line#*=}
    case "$value" in *[!A-Za-z0-9._:-]*) continue ;; esac
    case "$key" in
      STATE) STATE=$value ;;
      MODE) MODE=$value ;;
      STEP) STEP=$value ;;
      STARTED_AT) STARTED_AT=$value ;;
      FINISHED_AT) FINISHED_AT=$value ;;
      RUN_BOOT_ID) RUN_BOOT_ID=$value ;;
      PACKAGES) PACKAGES=$value ;;
      FIRMWARE) FIRMWARE=$value ;;
      REBOOT_REQUIRED) REBOOT_REQUIRED=$value ;;
      FAILED_STEP) FAILED_STEP=$value ;;
    esac
  done <"$STATE_FILE"
}

print_state() {
  printf 'STATE=%s\nMODE=%s\nSTEP=%s\nSTARTED_AT=%s\nFINISHED_AT=%s\n' \
    "$STATE" "$MODE" "$STEP" "$STARTED_AT" "$FINISHED_AT"
  printf 'RUN_BOOT_ID=%s\nPACKAGES=%s\nFIRMWARE=%s\nREBOOT_REQUIRED=%s\n' \
    "$RUN_BOOT_ID" "$PACKAGES" "$FIRMWARE" "$REBOOT_REQUIRED"
  printf 'FAILED_STEP=%s\n' "$FAILED_STEP"
}

save_state() {
  mkdir -p "$STATE_DIR"
  chmod 0755 "$STATE_DIR"
  local tmp
  tmp=$(mktemp "$STATE_DIR/.state.XXXXXX")
  print_state >"$tmp"
  chmod 0644 "$tmp"
  mv -f "$tmp" "$STATE_FILE"
}

set_step() {
  STATE=running
  STEP=$1
  save_state
  printf '=== %s step %s\n' "$(date -Is)" "$1"
}

cmd_check() {
  require_root
  unit_active && fail update-running
  local mode
  mode=$(relay_mode) || fail relay-unreachable
  prepare_apt "$mode"
  apt_relay -qq --error-on=any update >/dev/null 2>&1 || fail apt-update-failed
  refresh_firmware_or_skip "$mode" >/dev/null || fail firmware-refresh-failed
  printf 'PACKAGES=%s\n' "$(count_lines "$(pending_packages)")"
  printf 'FIRMWARE=%s\n' "$(firmware_count)"
  printf 'REBOOT_REQUIRED=%s\n' "$(reboot_requested)"
}

# Start a download or apply run in a transient unit, so that it outlives the
# SSH session that requested it.
start_run() {
  require_root
  if unit_active; then
    load_state
    printf 'ALREADY_RUNNING=1\n'
    print_state
    return 0
  fi
  relay_mode >/dev/null || fail relay-unreachable
  STATE=running
  MODE=$1
  STEP=starting
  STARTED_AT=$(date +%s)
  RUN_BOOT_ID=$(boot_id)
  save_state
  if ! systemd-run --quiet --unit="$UNIT" --collect --property=Type=exec \
    --description="DGX Spark Controller system $1" "$HELPER" "run-$1" >/dev/null; then
    STATE=failed
    FAILED_STEP=starting
    FINISHED_AT=$(date +%s)
    save_state
    fail unit-start-failed
  fi
  print_state
}

finish_run() {
  FINISHED_AT=$(date +%s)
  if [ -n "$FAILED_STEP" ]; then
    STATE=failed
  else
    STATE=succeeded
    STEP=complete
  fi
  save_state
  printf '=== %s %s %s\n' "$(date -Is)" "$MODE" "$STATE"
}

# Shared start of both run bodies: refresh lists and firmware metadata.
begin_run() {
  require_root
  [ -n "${INVOCATION_ID:-}" ] || fail not-in-unit
  export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l NEEDRESTART_SUSPEND=1
  export APT_LISTCHANGES_FRONTEND=none
  load_state
  : >"$LOG_FILE"
  chmod 0640 "$LOG_FILE"
  exec >>"$LOG_FILE" 2>&1
  set_step apt-update
  local mode
  if mode=$(relay_mode); then
    prepare_apt "$mode" || FAILED_STEP=apt-update
  else
    FAILED_STEP=relay
  fi
  if [ -z "$FAILED_STEP" ]; then
    apt_relay --error-on=any update || FAILED_STEP=apt-update
  fi
  if [ -z "$FAILED_STEP" ]; then
    set_step firmware-refresh
    refresh_firmware_or_skip "$mode" || FAILED_STEP=firmware-refresh
  fi
}

cmd_run_download() {
  begin_run
  local packages
  if [ -z "$FAILED_STEP" ]; then
    packages=$(pending_packages)
    PACKAGES=$(count_lines "$packages")
    set_step apt-download
    apt_relay -y --download-only full-upgrade || FAILED_STEP=apt-download
  fi
  FIRMWARE=$(firmware_count)
  if [ -z "$FAILED_STEP" ] && [ "$FIRMWARE" -gt 0 ]; then
    set_step firmware-download
    firmware_download || FAILED_STEP=firmware-download
  fi
  finish_run
}

cmd_run_apply() {
  begin_run
  local packages reboot_packages=0
  if [ -z "$FAILED_STEP" ]; then
    set_step dpkg-configure
    dpkg --configure -a || FAILED_STEP=dpkg-configure
  fi
  if [ -z "$FAILED_STEP" ]; then
    packages=$(pending_packages)
    PACKAGES=$(count_lines "$packages")
    reboot_packages=$(printf '%s\n' "$packages" | grep -cE "$REBOOT_PACKAGES" || true)
    set_step apt-upgrade
    apt_relay -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold \
      full-upgrade || FAILED_STEP=apt-upgrade
  fi
  FIRMWARE=0
  if [ -z "$FAILED_STEP" ]; then
    FIRMWARE=$(firmware_count)
    if [ "$FIRMWARE" -gt 0 ]; then
      set_step firmware-download
      firmware_download || FAILED_STEP=firmware-download
    fi
    if [ -z "$FAILED_STEP" ] && [ "$FIRMWARE" -gt 0 ]; then
      set_step firmware-update
      firmware_install || FAILED_STEP=firmware-update
    fi
  fi
  REBOOT_REQUIRED=$(reboot_requested)
  if [ "$reboot_packages" -gt 0 ] || [ "$FIRMWARE" -gt 0 ]; then
    REBOOT_REQUIRED=1
  fi
  finish_run
}

cmd_status() {
  require_root
  load_state
  # A run whose unit is gone without a result was killed or crashed.
  if [ "$STATE" = running ] && ! unit_active; then
    STATE=interrupted
  fi
  printf 'HELPER_VERSION=%s\n' "$HELPER_VERSION"
  print_state
  printf 'CURRENT_BOOT_ID=%s\n' "$(boot_id)"
  if [ "$STATE" = failed ] || [ "$STATE" = interrupted ]; then
    printf 'LOG_TAIL=%s\n' "$(tail -n 30 "$LOG_FILE" 2>/dev/null | base64 -w0 || true)"
  fi
}

cmd_reboot() {
  require_root
  unit_active && fail update-running
  printf 'BOOT_ID=%s\n' "$(boot_id)"
  if ! systemctl is-active --quiet "$REBOOT_UNIT.timer"; then
    systemd-run --quiet --unit="$REBOOT_UNIT" --collect --on-active=5 \
      systemctl reboot >/dev/null || fail reboot-schedule-failed
  fi
  printf 'REBOOT_SCHEDULED=1\n'
}

cmd_install() {
  require_root
  local user=${SUDO_USER:-}
  case "$user" in '' | root | *[!a-z0-9_.-]*) fail invalid-sudo-user ;; esac
  id -u -- "$user" >/dev/null 2>&1 || fail unknown-user
  unit_active && fail update-running
  local source tmp_helper tmp_sudoers command
  source=$(readlink -f -- "$0")
  [ -f "$source" ] || fail source-missing
  # Temporary names contain a dot, which sudo's includedir ignores, and the
  # final renames are atomic, so neither file is ever read half-written.
  tmp_helper=$(mktemp /usr/local/sbin/.dgx-spark-controller-update.XXXXXX)
  cp -- "$source" "$tmp_helper"
  chown root:root "$tmp_helper"
  chmod 0755 "$tmp_helper"
  mv -f -- "$tmp_helper" "$HELPER"
  tmp_sudoers=$(mktemp /etc/sudoers.d/.dgx-spark-controller-update.XXXXXX)
  {
    printf '# Installed by DGX Spark Controller: %s may run only the update helper.\n' \
      "$user"
    printf '%s ALL=(root) NOPASSWD:' "$user"
    for command in check download apply status reboot; do
      printf ' %s %s,' "$HELPER" "$command"
    done | sed 's/,$//'
    printf '\n'
  } >"$tmp_sudoers"
  chown root:root "$tmp_sudoers"
  chmod 0440 "$tmp_sudoers"
  if ! visudo -cqf "$tmp_sudoers" >/dev/null 2>&1; then
    rm -f -- "$tmp_sudoers"
    fail sudoers-invalid
  fi
  mv -f -- "$tmp_sudoers" "$SUDOERS"
  printf 'INSTALLED=1\nSHA256=%s\n' "$(sha256sum "$HELPER" | cut -d' ' -f1)"
}

cmd_uninstall() {
  require_root
  unit_active && fail update-running
  rm -f -- "$SUDOERS" "$HELPER"
  rm -rf -- "$STATE_DIR"
  printf 'UNINSTALLED=1\n'
}

[ "$#" -eq 1 ] || fail usage 64
case "$1" in
  check) cmd_check ;;
  download) start_run download ;;
  apply) start_run apply ;;
  run-download) cmd_run_download ;;
  run-apply) cmd_run_apply ;;
  status) cmd_status ;;
  reboot) cmd_reboot ;;
  install) cmd_install ;;
  uninstall) cmd_uninstall ;;
  version) printf 'HELPER_VERSION=%s\n' "$HELPER_VERSION" ;;
  *) fail usage 64 ;;
esac
