#!/usr/bin/env bash
set -euo pipefail

ENGINE="${ENGINE:-}"
ENV="${ENV:-dev}"
if [[ -z "$ENGINE" ]]; then
  if command -v podman >/dev/null 2>&1; then
    ENGINE=podman
  else
    ENGINE=docker
  fi
fi
case "$ENGINE" in
  docker|podman) ;;
  *) echo "ENGINE must be docker or podman" >&2; exit 2 ;;
esac
case "$ENV" in
  dev|staging) ;;
  *) echo "ENV must be dev or staging" >&2; exit 2 ;;
esac
command -v "$ENGINE" >/dev/null 2>&1 || { echo "$ENGINE is required" >&2; exit 1; }

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
IMAGE="localhost/exchange-process:$ENV"
echo "Building $IMAGE with $ENGINE"
exec "$ENGINE" build --tag "$IMAGE" "$ROOT"
