#!/bin/sh
# Run the local service from a repository checkout's .venv.
set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
cd "$repo_dir"
exec "$repo_dir/.venv/bin/python" -m controller "$@"
