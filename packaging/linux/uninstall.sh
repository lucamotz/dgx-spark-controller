#!/bin/bash
# Remove what packaging/linux/install.sh added for the current Linux user.
#
# Removes the desktop entry, icon and systemd user unit. The virtualenv is
# removed only with --remove-venv and only if install.sh created it. The
# controller's configuration, journal and logs are never touched.
#
# Usage: packaging/linux/uninstall.sh [--remove-venv]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
venv="$repo/.venv"
slug="dgx-spark-controller"
venv_marker="$venv/.$slug-installer"
remove_venv=0

usage() {
  sed -n '2,8p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

xdg_dir() {
  local value="$1" fallback="$2"
  if [[ "$value" == /* ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "$fallback"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remove-venv) remove_venv=1 ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

[[ -n "${HOME:-}" ]] || { echo "error: HOME is not set" >&2; exit 1; }
data_home="$(xdg_dir "${XDG_DATA_HOME:-}" "$HOME/.local/share")"
config_home="$(xdg_dir "${XDG_CONFIG_HOME:-}" "$HOME/.config")"
state_home="$(xdg_dir "${XDG_STATE_HOME:-}" "$HOME/.local/state")"

unit_file="$config_home/systemd/user/$slug.service"
if [[ -f "$unit_file" ]]; then
  if grep -q "packaging/linux/install.sh" "$unit_file"; then
    if command -v systemctl >/dev/null 2>&1; then
      # Stops only the local controller; remote models keep serving.
      systemctl --user disable --now "$slug.service" >/dev/null 2>&1 || true
    fi
    rm -f "$unit_file"
    if command -v systemctl >/dev/null 2>&1; then
      systemctl --user daemon-reload || true
    fi
    echo "Removed $unit_file"
  else
    echo "Kept $unit_file: it was not written by install.sh" >&2
  fi
fi

desktop_file="$data_home/applications/$slug.desktop"
if [[ -f "$desktop_file" ]]; then
  if grep -q "^X-DGX-Spark-Controller-Installer=" "$desktop_file"; then
    rm -f "$desktop_file"
    if command -v update-desktop-database >/dev/null 2>&1; then
      update-desktop-database -q "$data_home/applications" || true
    fi
    echo "Removed $desktop_file"
  else
    echo "Kept $desktop_file: it was not written by install.sh" >&2
  fi
fi

icon_file="$data_home/icons/hicolor/256x256/apps/$slug.png"
if [[ -f "$icon_file" ]]; then
  rm -f "$icon_file"
  if command -v gtk-update-icon-cache >/dev/null 2>&1; then
    gtk-update-icon-cache -q -t -f "$data_home/icons/hicolor" || true
  fi
  echo "Removed $icon_file"
fi

if [[ "$remove_venv" == 1 ]]; then
  if [[ -f "$venv_marker" ]]; then
    rm -rf "$venv"
    echo "Removed $venv"
  else
    echo "Kept $venv: it was not created by install.sh" >&2
  fi
fi

cat <<EOF

Controller configuration, journal and logs were kept (under
\$DGX_CONTROLLER_HOME instead, if you set it):
  $config_home/$slug
  $data_home/$slug
  $state_home/$slug
Delete them manually if you no longer need them.
EOF
