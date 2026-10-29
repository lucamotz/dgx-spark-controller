#!/usr/bin/env bash
# DGX Spark Controller - generic vLLM release recipe: prepare this node.
#
# Pulls the digest-pinned vLLM release image, downloads the pinned Hugging Face
# revision into the node's model root, verifies every expected weight file by
# size, and reports results as KEY=VALUE lines in STATUS_FILE. It never touches
# a running model. A Hugging Face token, if the operator stored one on this node
# with `hf auth login`, is mounted read-only; the controller never sees it.
set -uo pipefail

: "${PREPARE_ENV:?PREPARE_ENV must name the rendered prepare environment}"
test -r "$PREPARE_ENV" || exit 2

allowed_key() {
  case "$1" in
    PREPARE_ID|VLLM_IMAGE|HF_REPO|HF_REVISION|MODEL_ROOT|MODEL_DIR) return 0 ;;
    STATUS_FILE|EXPECTED_FILES|REQUIRED_FREE_BYTES|CHECK_MULTINODE) return 0 ;;
  esac
  return 1
}
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  key=${line%%=*}
  value=${line#*=}
  allowed_key "$key" || { echo "unexpected prepare key: $key" >&2; exit 2; }
  case "$value" in *[!A-Za-z0-9._:/@+-]*|'') echo "unsafe prepare value for $key" >&2; exit 2 ;; esac
  printf -v "$key" '%s' "$value"
done < "$PREPARE_ENV"

report() { printf '%s\n' "$@" >> "$STATUS_FILE"; }
fail() { report "MESSAGE=$1" "STATE=failed"; exit 1; }

: > "$STATUS_FILE"
report "STATE=running" "STEP=checking"
target="$MODEL_ROOT/$MODEL_DIR"
mkdir -p "$MODEL_ROOT" || fail "model root cannot be created"
report "MEM_TOTAL=$(awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo)"
report "HOME_DIR=$HOME"

complete=1
while read -r size name; do
  [ -n "$name" ] || continue
  if [ "$(stat -c %s "$target/$name" 2>/dev/null || echo missing)" != "$size" ]; then complete=0; break; fi
done < "$EXPECTED_FILES"
if [ "$complete" = 0 ]; then
  available=$(df -B1 --output=avail "$MODEL_ROOT" | tail -1 | tr -d ' ')
  if [ "$available" -lt "$REQUIRED_FREE_BYTES" ]; then fail "not enough free disk space in the model root"; fi
fi

report "STEP=pulling"
docker pull --quiet "$VLLM_IMAGE" >/dev/null || fail "runtime image pull failed"
image_id=$(docker image inspect --format '{{.Id}}' "$VLLM_IMAGE") || fail "runtime image is not inspectable"
report "IMAGE_ID=$image_id"
# Read package metadata instead of running the vllm CLI: recent releases infer
# the GPU while building their argument parser, and prepare never claims the GPU
# so that it cannot disturb a model that is still serving.
report "STEP=inspecting"
facts=$(docker run --rm --network none --memory 1g --entrypoint "" "$VLLM_IMAGE" \
  python3 -I -c 'import importlib.metadata as m, os
d = m.distribution("vllm")
src = next((f for f in d.files or () if str(f).endswith("vllm/engine/arg_utils.py")), None)
print("VERSION=" + d.version)
print("NNODES=" + ("1" if src is not None and "nnodes" in src.read_text() else "0"))
print("CLI=" + ("1" if os.access("/usr/local/bin/vllm", os.X_OK) else "0"))' 2>/dev/null) \
  || fail "runtime image has no readable vLLM installation"
version=$(printf '%s\n' "$facts" | sed -n 's/^VERSION=//p' | tr -cd 'A-Za-z0-9._+-')
[ -n "$version" ] || fail "runtime image has no readable vLLM installation"
printf '%s\n' "$facts" | grep -qx 'CLI=1' || fail "runtime image has no vllm executable where the launcher expects it"
report "VLLM_VERSION=$version"
if [ "$CHECK_MULTINODE" = 1 ] && ! printf '%s\n' "$facts" | grep -qx 'NNODES=1'; then
  fail "this vLLM release does not support multi-node serving (no nnodes option)"
fi

if [ "$complete" = 0 ]; then
  report "STEP=downloading"
  token_args=()
  if [ -r "$HOME/.cache/huggingface/token" ]; then
    token_args=(--volume "$HOME/.cache/huggingface/token:/run/hf-token:ro" --env HF_TOKEN_PATH=/run/hf-token)
  fi
  mkdir -p "$target" || fail "model directory cannot be created"
  docker run --rm --name "dgxc-prepare-$PREPARE_ID" --user "$(id -u):$(id -g)" --memory 4g \
    --env HOME=/tmp --env HF_HUB_DISABLE_TELEMETRY=1 --env HF_HUB_DISABLE_PROGRESS_BARS=1 \
    "${token_args[@]}" --volume "$MODEL_ROOT:/models" --entrypoint "" "$VLLM_IMAGE" \
    python3 -c 'import sys
from huggingface_hub import snapshot_download
snapshot_download(repo_id=sys.argv[1], revision=sys.argv[2], local_dir=sys.argv[3], ignore_patterns=["original/*"])' \
    "$HF_REPO" "$HF_REVISION" "/models/$MODEL_DIR" >/dev/null 2>&1 || fail "weight download failed (gated model without a token on this node?)"
fi

report "STEP=verifying"
while read -r size name; do
  [ -n "$name" ] || continue
  actual=$(stat -c %s "$target/$name" 2>/dev/null || echo missing)
  [ "$actual" = "$size" ] || fail "weight file size mismatch: $name"
done < "$EXPECTED_FILES"
test -r "$target/config.json" || fail "config.json is missing"
manifest="$target/config.json"
if [ -r "$target/model.safetensors.index.json" ]; then manifest="$target/model.safetensors.index.json"; fi
report "MANIFEST_PATH=${manifest#"$target/"}"
report "MANIFEST_SHA256=$(sha256sum "$manifest" | awk '{print $1}')"
# Every weight file is verified, so huggingface_hub temp files that a cancelled
# earlier download left and this one did not resume are garbage. Recent ones
# may belong to a concurrent download of the same weights and stay.
find "$target/.cache/huggingface/download" -maxdepth 1 -type f -name '*.incomplete' \
  -mmin +2 -delete 2>/dev/null || true
report "STEP=done" "STATE=succeeded"
