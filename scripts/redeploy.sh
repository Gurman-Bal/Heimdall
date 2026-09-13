#!/usr/bin/env bash
# scripts/redeploy.sh
#
# Hard-clean rebuild + deploy for the TrueNAS host. Run this after every
# `git pull` when you want to be certain you're running exactly what's in
# the repo — no cached layers, no leftover containers/images/build cache.
#
# Usage (from the repo root on TrueNAS):
#   ./scripts/redeploy.sh
#
# What it deliberately does NOT touch: the `heimdall-data` and
# `ollama-data` named volumes — your database and pulled models survive a
# redeploy. Pass --wipe-data if you ever want to nuke those too.

set -euo pipefail

WIPE_DATA=false
if [[ "${1:-}" == "--wipe-data" ]]; then
    WIPE_DATA=true
fi

cd "$(dirname "$0")/.."

echo "== 1. Pulling latest code =="
git pull

echo "== 2. Stopping and removing containers (and any orphans) =="
if $WIPE_DATA; then
    echo "   --wipe-data set: removing named volumes too"
    docker compose down --remove-orphans -v
else
    docker compose down --remove-orphans
fi

echo "== 3. Clearing build cache and dangling images =="
docker builder prune -f
docker image prune -f

echo "== 4. Rebuilding from scratch (--no-cache, --pull) =="
docker compose build --no-cache --pull

echo "== 5. Starting the stack =="
docker compose up -d

echo "== 6. Current state =="
docker compose ps

echo ""
echo "Done. Tail logs with: docker compose logs -f worker controller"