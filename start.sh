#!/bin/bash
set -euo pipefail

mkdir -p data logs
chmod 777 data logs 2>/dev/null || true

docker compose pull
docker compose down
docker compose up -d
