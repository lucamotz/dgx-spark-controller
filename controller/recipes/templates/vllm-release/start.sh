#!/usr/bin/env bash
# DGX Spark Controller - generic vLLM release recipe: start one rank.
#
# The controller installs this file unchanged and verifies its SHA-256 before
# every run. Its only input is the controller-rendered ENV_FILE. That file is
# parsed as data (an explicit key allowlist with a safe character set), never
# sourced or evaluated, so a recipe value can never become shell code.
set -euo pipefail

: "${ENV_FILE:?ENV_FILE must name the rendered recipe environment}"
test -r "$ENV_FILE"

allowed_key() {
  case "$1" in
    RECIPE_FORMAT|CONTAINER_NAME|DEPLOYMENT_LABEL|VLLM_IMAGE|VLLM_HEAD_IMAGE_ID) return 0 ;;
    WORKER_VLLM_IMAGE_ID|MODEL_ROOT|MODEL_DIR|SERVED_MODEL_NAME|PORT) return 0 ;;
    TENSOR_PARALLEL_SIZE|NNODES|MASTER_ADDR|MASTER_PORT|MAX_MODEL_LEN) return 0 ;;
    MAX_NUM_SEQS|MAX_NUM_BATCHED_TOKENS|GPU_MEMORY_UTILIZATION|KV_CACHE_DTYPE) return 0 ;;
    ENABLE_PREFIX_CACHING|TRUST_REMOTE_CODE|ENFORCE_EAGER|TOOL_CALL_PARSER) return 0 ;;
    REASONING_PARSER|CACHE_VOLUME|NODE_RANK|VLLM_HOST_IP) return 0 ;;
  esac
  return 1
}
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  key=${line%%=*}
  value=${line#*=}
  allowed_key "$key" || { echo "unexpected recipe key: $key" >&2; exit 2; }
  case "$value" in *[!A-Za-z0-9._:/@+-]*|'') echo "unsafe recipe value for $key" >&2; exit 2 ;; esac
  printf -v "$key" '%s' "$value"
done < "$ENV_FILE"

for required in RECIPE_FORMAT CONTAINER_NAME DEPLOYMENT_LABEL VLLM_IMAGE VLLM_HEAD_IMAGE_ID \
  WORKER_VLLM_IMAGE_ID MODEL_ROOT MODEL_DIR SERVED_MODEL_NAME PORT TENSOR_PARALLEL_SIZE NNODES \
  MASTER_ADDR MASTER_PORT MAX_MODEL_LEN MAX_NUM_SEQS MAX_NUM_BATCHED_TOKENS GPU_MEMORY_UTILIZATION \
  KV_CACHE_DTYPE ENABLE_PREFIX_CACHING TRUST_REMOTE_CODE ENFORCE_EAGER TOOL_CALL_PARSER \
  REASONING_PARSER CACHE_VOLUME NODE_RANK VLLM_HOST_IP; do
  if [ -z "${!required:-}" ]; then echo "missing recipe key: $required" >&2; exit 2; fi
done
test "$RECIPE_FORMAT" = 1

# Never replace or adopt an existing container; the controller verified the
# node idle before calling this script and stop.sh removes only its own rank.
if docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "container $CONTAINER_NAME already exists; refusing to start a second rank" >&2
  exit 3
fi

expected_image_id=$VLLM_HEAD_IMAGE_ID
if [ "$NODE_RANK" != 0 ]; then expected_image_id=$WORKER_VLLM_IMAGE_ID; fi
actual_image_id=$(docker image inspect --format '{{.Id}}' "$VLLM_IMAGE")
if [ "$actual_image_id" != "$expected_image_id" ]; then
  echo "pinned runtime image id mismatch: $actual_image_id" >&2
  exit 4
fi
test -r "$MODEL_ROOT/$MODEL_DIR/config.json"

args=(run --detach --name "$CONTAINER_NAME" --restart no
  --label "io.github.dgx-spark-controller.deployment=$DEPLOYMENT_LABEL"
  --label "io.github.dgx-spark-controller.rank=$NODE_RANK"
  --network host --ipc host --gpus all
  --ulimit memlock=-1 --ulimit stack=67108864
  --volume "$MODEL_ROOT:/models:ro"
  --volume "$CACHE_VOLUME:/root/.cache"
  --env "MAX_MODEL_LEN=$MAX_MODEL_LEN"
  --env "MAX_NUM_SEQS=$MAX_NUM_SEQS"
  --env "MAX_NUM_BATCHED_TOKENS=$MAX_NUM_BATCHED_TOKENS"
  --env "GPU_MEMORY_UTILIZATION=$GPU_MEMORY_UTILIZATION"
  --env "KV_CACHE_DTYPE=$KV_CACHE_DTYPE"
  --env "ENFORCE_EAGER=$ENFORCE_EAGER")
if [ -e /dev/infiniband ]; then args+=(--device /dev/infiniband:/dev/infiniband); fi

if [ "$NNODES" -gt 1 ]; then
  # Bind collective traffic to the interface that owns this rank's cluster IP
  # (normally a ConnectX-7 port on the direct Spark-to-Spark link).
  interface=$(ip -o -4 addr show | awk -v ip="$VLLM_HOST_IP" '{ split($4, a, "/"); if (a[1] == ip) { print $2; exit } }')
  if [ -z "$interface" ]; then echo "no local interface owns cluster address $VLLM_HOST_IP" >&2; exit 5; fi
  args+=(--env "VLLM_HOST_IP=$VLLM_HOST_IP"
    --env "NCCL_SOCKET_IFNAME=$interface"
    --env "GLOO_SOCKET_IFNAME=$interface"
    --env "TP_SOCKET_IFNAME=$interface"
    --env "NCCL_IB_DISABLE=0")
fi

args+=(--entrypoint "" "$VLLM_IMAGE"
  /usr/local/bin/vllm serve "/models/$MODEL_DIR"
  --served-model-name "$SERVED_MODEL_NAME"
  --host 0.0.0.0
  --port "$PORT"
  --tensor-parallel-size "$TENSOR_PARALLEL_SIZE"
  --max-model-len "$MAX_MODEL_LEN"
  --max-num-seqs "$MAX_NUM_SEQS"
  --max-num-batched-tokens "$MAX_NUM_BATCHED_TOKENS"
  --gpu-memory-utilization "$GPU_MEMORY_UTILIZATION"
  --kv-cache-dtype "$KV_CACHE_DTYPE")
if [ "$ENABLE_PREFIX_CACHING" = 1 ]; then args+=(--enable-prefix-caching); fi
if [ "$TRUST_REMOTE_CODE" = 1 ]; then args+=(--trust-remote-code); fi
if [ "$ENFORCE_EAGER" = 1 ]; then args+=(--enforce-eager); fi
if [ "$TOOL_CALL_PARSER" != none ]; then args+=(--enable-auto-tool-choice --tool-call-parser "$TOOL_CALL_PARSER"); fi
if [ "$REASONING_PARSER" != none ]; then args+=(--reasoning-parser "$REASONING_PARSER"); fi
if [ "$NNODES" -gt 1 ]; then
  args+=(--distributed-executor-backend mp --nnodes "$NNODES" --node-rank "$NODE_RANK"
    --master-addr "$MASTER_ADDR" --master-port "$MASTER_PORT")
  if [ "$NODE_RANK" != 0 ]; then args+=(--headless); fi
fi

docker "${args[@]}" >/dev/null
echo "started rank $NODE_RANK as $CONTAINER_NAME"
