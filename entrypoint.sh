#!/bin/sh
# Fix bind-mount directory issue and ownership, then start proxyweave

OVERRIDE_CONFIG=false
CONFIG_PATH="/etc/proxyweave/config.yaml"

# Check if config.yaml was bind-mounted as a directory (Docker creates directories
# for non-existent bind-mount sources)
if [ -d "/etc/proxyweave/config.yaml" ]; then
  echo "=======================================================" >&2
  echo "WARNING: config.yaml is a directory, not a file!" >&2
  echo "This happens when Docker creates the bind-mount target" >&2
  echo "before the file exists on the host." >&2
  echo "" >&2
  echo "To fix permanently, run on the host:" >&2
  echo "  docker compose down && rm -rf config.yaml && touch config.yaml && docker compose up -d" >&2
  echo "Or use start.sh which handles this automatically." >&2
  echo "=======================================================" >&2

  CONFIG_PATH="/tmp/default_config.yaml"
  cat > "$CONFIG_PATH" <<'YAML'
mode: pool

listener:
  address: 0.0.0.0
  port: 2323

pool:
  mode: balance

management:
  enabled: true
  listen: 0.0.0.0:9091
  password: ""

dns:
  server: 223.5.5.5
  port: 53
  strategy: prefer_ipv4
YAML
  OVERRIDE_CONFIG=true
  echo "INFO: Using fallback config at $CONFIG_PATH" >&2
fi

# Check if nodes.txt was bind-mounted as a directory
if [ -d "/etc/proxyweave/nodes.txt" ]; then
  echo "=======================================================" >&2
  echo "WARNING: nodes.txt is a directory, not a file!" >&2
  echo "This happens when Docker creates the bind-mount target" >&2
  echo "before the file exists on the host." >&2
  echo "" >&2
  echo "To fix permanently, run on the host:" >&2
  echo "  docker compose down && rm -rf nodes.txt && touch nodes.txt && docker compose up -d" >&2
  echo "Or use start.sh which handles this automatically." >&2
  echo "=======================================================" >&2
fi

# Fix ownership of mounted config directory so the non-root user can write
chown -R easy:easy /etc/proxyweave 2>/dev/null || true
chown -R easy:easy /app 2>/dev/null || true

if [ "$OVERRIDE_CONFIG" = "true" ]; then
  chown easy:easy "$CONFIG_PATH" 2>/dev/null || true
  exec su-exec easy /usr/local/bin/proxyweave --config "$CONFIG_PATH"
else
  exec su-exec easy /usr/local/bin/proxyweave "$@"
fi
