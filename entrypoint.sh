#!/bin/sh
# Prepare writable runtime paths, then start proxyweave.

set -eu

mkdir -p /app/data /app/logs
chown -R easy:easy /app 2>/dev/null || true

exec su-exec easy /usr/local/bin/proxyweave "$@"
