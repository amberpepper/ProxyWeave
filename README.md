# ProxyWeave

[简体中文](README_ZH.md)

A sing-box based proxy pool manager.

## Acknowledgement

This project is based on [jasonwong1991/easy_proxies](https://github.com/jasonwong1991/easy_proxies)
and is further developed as a customized fork.

Original author: [jasonwong1991](https://github.com/jasonwong1991)

Current project: [amberpepper/ProxyWeave](https://github.com/amberpepper/ProxyWeave)

## Highlights

- Web dashboard (`/`) and management APIs (`/api/*`)
- Real-time traffic chart via SSE (`/api/traffic`) and log console (`/api/logs`)
- Node CRUD, single/batch probing, manual blacklist/release
- SQLite-backed settings, nodes, subscriptions, and runtime state persistence
- Per-subscription refresh scheduling with node association and node counts
- GeoIP region routing (e.g. `/jp`, `/us`) with GeoLite2 auto-update

## Install

```bash
git clone https://github.com/amberpepper/ProxyWeave.git
cd ProxyWeave
mkdir -p data logs
```

## Start

### Docker (recommended)

```bash
./start.sh
# or
docker compose up -d
```

### GHCR image

```bash
mkdir -p data logs

docker pull ghcr.io/amberpepper/proxyweave:latest

docker run -d \
  --name proxyweave \
  --network host \
  --restart unless-stopped \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  ghcr.io/amberpepper/proxyweave:latest
```

If you do not want host networking, map ports explicitly, for example:

```bash
docker run -d \
  --name proxyweave \
  --restart unless-stopped \
  -p 2323:2323 \
  -p 9091:9091 \
  -p 24000-24100:24000-24100 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  ghcr.io/amberpepper/proxyweave:latest
```

### Run from source

```bash
mkdir -p data logs

go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" ./cmd/proxyweave
```

## Access

- WebUI: `http://localhost:9091`
- Default proxy (pool): `127.0.0.1:2323`

## Notes

- SQLite is now the only source of truth.
- Nodes, subscriptions, and runtime health state persist across restarts.
- Manage subscriptions directly from **Node Management → Subscription Management**.

## Stop

```bash
docker compose down
```
