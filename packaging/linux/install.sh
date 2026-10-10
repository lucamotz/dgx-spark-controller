#!/bin/bash
# Install DGX Spark Controller for the current Linux user (no sudo).
#
# Creates <repo>/.venv if it is missing, installs this checkout in editable
# mode, and adds a desktop entry and icon. --with-window also installs the
# GTK window backend; --service writes a systemd user unit without enabling it.
#
# Usage: packaging/linux/install.sh [--with-window] [--service]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
venv="$repo/.venv"
python="$venv/bin/python"
slug="dgx-spark-controller"
venv_marker="$venv/.$slug-installer"

with_window=0
with_service=0

usage() {
  sed -n '2,8p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

die() {
  echo "error: $*" >&2
  exit 1
}

xdg_dir() {
  # XDG base directories must be absolute; relative values are ignored.
  local value="$1" fallback="$2"
  if [[ "$value" == /* ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "$fallback"
  fi
}

quote_path() {
  # Quote a path for a desktop-entry Exec= or systemd ExecStart= line.
  local path="$1"
  if [[ "$path" == *[[:cntrl:]\"\`\$\\%]* ]]; then
    die "unsupported character in path: $path"
  fi
  if [[ "$path" =~ ^[A-Za-z0-9_./+-]+$ ]]; then
    printf '%s\n' "$path"
  else
    printf '"%s"\n' "$path"
  fi
}

render() {
  # render TEMPLATE OUTPUT KEY=VALUE...: replace each @KEY@ with VALUE
  # literally (no pattern-replacement semantics, so '&' and '\' are safe).
  local template="$1" output="$2" line pair key value rendered
  shift 2
  : >"$output"
  while IFS= read -r line || [[ -n "$line" ]]; do
    for pair in "$@"; do
      key="@${pair%%=*}@"
      value="${pair#*=}"
      rendered=""
      while [[ "$line" == *"$key"* ]]; do
        rendered+="${line%%"$key"*}$value"
        line="${line#*"$key"}"
      done
      line="$rendered$line"
    done
    printf '%s\n' "$line" >>"$output"
  done <"$template"
}

print_gtk_packages() {
  cat <<'EOF'
The GTK window backend builds PyGObject and needs system packages. Install
them with your package manager, for example:
  Debian/Ubuntu: sudo apt install libgirepository-2.0-dev libcairo2-dev pkg-config python3-dev gir1.2-webkit2-4.1
  Fedora:        sudo dnf install gobject-introspection-devel cairo-gobject-devel pkgconf-pkg-config python3-devel webkit2gtk4.1
(Ubuntu 22.04 and older ship libgirepository1.0-dev instead.)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-window) with_window=1 ;;
    --service) with_service=1 ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
  shift
done

[[ "$(uname -s)" == "Linux" ]] || die "this installer is for Linux; on macOS use packaging/macos/build_app.sh"
[[ "$(id -u)" != "0" ]] || die "run this installer as your normal user, not root"
[[ -n "${HOME:-}" ]] || die "HOME is not set"

data_home="$(xdg_dir "${XDG_DATA_HOME:-}" "$HOME/.local/share")"
config_home="$(xdg_dir "${XDG_CONFIG_HOME:-}" "$HOME/.config")"

app_exec="$venv/bin/$slug-app"
service_exec="$venv/bin/$slug"
quoted_app_exec="$(quote_path "$app_exec")"
quoted_service_exec="$(quote_path "$service_exec")"

if [[ ! -x "$python" ]]; then
  echo "Creating $venv"
  if command -v uv >/dev/null 2>&1; then
    uv venv --python 3.12 "$venv"
  else
    command -v python3 >/dev/null 2>&1 || die "python3 (3.12 or newer) is required"
    python3 -m venv "$venv"
  fi
  : >"$venv_marker"
fi
"$python" -c 'import sys; sys.exit(sys.version_info < (3, 12))' ||
  die "$python is older than Python 3.12; remove $venv and re-run"

spec="$repo"
if [[ "$with_window" == 1 ]]; then
  spec="${repo}[desktop-gtk]"
  print_gtk_packages
fi
echo "Installing $spec in editable mode"
install_ok=1
if command -v uv >/dev/null 2>&1; then
  uv pip install --python "$python" -e "$spec" || install_ok=0
else
  if ! "$python" -m pip --version >/dev/null 2>&1; then
    "$python" -m ensurepip --upgrade >/dev/null
  fi
  "$python" -m pip install -e "$spec" || install_ok=0
fi
if [[ "$install_ok" != 1 ]]; then
  [[ "$with_window" != 1 ]] || print_gtk_packages >&2
  die "package installation failed"
fi

icon_dir="$data_home/icons/hicolor/256x256/apps"
mkdir -p "$icon_dir"
install -m 0644 "$repo/controller/assets/$slug-256.png" "$icon_dir/$slug.png"

apps_dir="$data_home/applications"
desktop_file="$apps_dir/$slug.desktop"
mkdir -p "$apps_dir"
render "$here/$slug.desktop.in" "$desktop_file" \
  "EXEC=$quoted_app_exec" "TRY_EXEC=$app_exec"
chmod 0644 "$desktop_file"
if command -v desktop-file-validate >/dev/null 2>&1; then
  desktop-file-validate "$desktop_file" || echo "warning: desktop-file-validate reported problems" >&2
fi
if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database -q "$apps_dir" || true
fi
if command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q -t -f "$data_home/icons/hicolor" || true
fi
echo "Installed desktop entry $desktop_file"

if [[ "$with_service" == 1 ]]; then
  unit_dir="$config_home/systemd/user"
  unit_file="$unit_dir/$slug.service"
  mkdir -p "$unit_dir"
  render "$here/$slug.service.in" "$unit_file" "EXEC_START=$quoted_service_exec"
  chmod 0644 "$unit_file"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl --user daemon-reload || true
  fi
  echo "Installed systemd user unit $unit_file (not enabled)."
  echo "Start it now and at every login with:"
  echo "  systemctl --user enable --now $slug.service"
  echo "To keep it running while you are logged out: loginctl enable-linger"
fi

cat <<EOF

Done. Start the app from your desktop menu ("DGX Spark Controller") or run:
  $app_exec
Closing the app stops only the local service; remote models keep running.
EOF
