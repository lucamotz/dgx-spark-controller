#!/usr/bin/env bash
# DGX Spark Controller - generic vLLM release recipe: stop this node's rank.
#
# Removes only the container named by the recipe, and only when its ownership
# label matches this recipe's deployment. Anything else is reported, not killed.
set -euo pipefail

: "${ENV_FILE:?ENV_FILE must name the rendered recipe environment}"
test -r "$ENV_FILE"

CONTAINER_NAME=
DEPLOYMENT_LABEL=
while IFS= read -r line || [ -n "$line" ]; do
  key=${line%%=*}
  value=${line#*=}
  case "$value" in *[!A-Za-z0-9._:/@+-]*|'') continue ;; esac
  case "$key" in
    CONTAINER_NAME) CONTAINER_NAME=$value ;;
    DEPLOYMENT_LABEL) DEPLOYMENT_LABEL=$value ;;
  esac
done < "$ENV_FILE"
test -n "$CONTAINER_NAME"
test -n "$DEPLOYMENT_LABEL"

if ! docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "rank container $CONTAINER_NAME is already absent"
  exit 0
fi
owner=$(docker container inspect --format '{{index .Config.Labels "io.github.dgx-spark-controller.deployment"}}' "$CONTAINER_NAME")
if [ "$owner" != "$DEPLOYMENT_LABEL" ]; then
  echo "container $CONTAINER_NAME is not owned by this recipe; leaving it untouched" >&2
  exit 3
fi
docker stop --time 90 "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker rm --force "$CONTAINER_NAME" >/dev/null
echo "stopped and removed $CONTAINER_NAME"
