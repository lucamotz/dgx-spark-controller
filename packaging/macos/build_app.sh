#!/bin/bash
# Build build/DGXSparkController.app, a thin repository-backed macOS launcher.
#
# The bundle contains only Info.plist, an icon and a small shell script that
# runs "<repo>/.venv/bin/python -m controller.launcher"; the code stays in this
# checkout. Re-running the script replaces the previous bundle.
#
# Usage: packaging/macos/build_app.sh [--output DIR]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out_dir="$repo/build"
app_name="DGXSparkController"

usage() {
  sed -n '2,8p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -o | --output)
      [[ $# -ge 2 ]] || { echo "error: $1 needs a directory" >&2; exit 2; }
      out_dir="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "error: the app bundle can only be built on macOS" >&2
  exit 1
fi
for tool in sips iconutil plutil; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: $tool not found" >&2; exit 1; }
done
if [[ "$repo" == *$'\n'* ]]; then
  echo "error: the repository path must not contain a newline" >&2
  exit 1
fi

version="$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$repo/pyproject.toml" | head -n 1)"
if [[ ! "$version" =~ ^[0-9]+(\.[0-9]+)*([.+-][0-9A-Za-z.]+)?$ ]]; then
  echo "error: could not read the project version from pyproject.toml" >&2
  exit 1
fi

mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd)"
app="$out_dir/$app_name.app"
staging="$(mktemp -d "$out_dir/.$app_name.XXXXXX")"
trap 'rm -rf "$staging"' EXIT

bundle="$staging/$app_name.app"
mkdir -p "$bundle/Contents/MacOS" "$bundle/Contents/Resources"

sed "s/@VERSION@/$version/g" "$here/Info.plist.in" >"$bundle/Contents/Info.plist"
plutil -lint "$bundle/Contents/Info.plist" >/dev/null

launcher="$bundle/Contents/MacOS/$app_name"
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ "$line" == "baked_repo=@REPO@" ]]; then
    printf 'baked_repo=%q\n' "$repo"
  else
    printf '%s\n' "$line"
  fi
done <"$here/launcher.sh.in" >"$launcher"
chmod 755 "$launcher"
bash -n "$launcher"

iconset="$staging/AppIcon.iconset"
mkdir -p "$iconset"
source_png="$repo/controller/assets/dgx-spark-controller.png"
for size in 16 32 128 256 512; do
  sips -z "$size" "$size" "$source_png" --out "$iconset/icon_${size}x${size}.png" >/dev/null
  double=$((size * 2))
  sips -z "$double" "$double" "$source_png" \
    --out "$iconset/icon_${size}x${size}@2x.png" >/dev/null
done
iconutil -c icns "$iconset" -o "$bundle/Contents/Resources/AppIcon.icns"
printf 'APPL????' >"$bundle/Contents/PkgInfo"

rm -rf "$app"
mv "$bundle" "$app"
touch "$app"

if command -v codesign >/dev/null 2>&1; then
  xattr -cr "$app"
  codesign --force --deep --sign - "$app"
  codesign --verify --deep --strict "$app"
fi

if [[ ! -x "$repo/.venv/bin/python" ]]; then
  echo "warning: $repo/.venv/bin/python is missing; create it before opening the app" >&2
elif ! "$repo/.venv/bin/python" -c 'import webview' >/dev/null 2>&1; then
  echo "warning: pywebview is not installed; the app will fall back to the browser." >&2
  echo "         install it with: uv pip install --python .venv/bin/python -e '.[desktop]'" >&2
fi

echo "built $app"
