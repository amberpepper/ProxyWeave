# ProxyWeave

[简体中文](README_ZH.md)

A sing-box based proxy pool manager.

## Acknowledgement

This project is based on [jasonwong1991/easy_proxies](https://github.com/jasonwong1991/easy_proxies)
and is further developed as a customized fork.

Original author: [jasonwong1991](https://github.com/jasonwong1991)

Current project: [amberpepper/ProxyWeave](https://github.com/amberpepper/ProxyWeave)

## Highlights

This customized version includes:

- Web dashboard (`/`) and management APIs (`/api/*`)
- Real-time traffic chart via SSE (`/api/traffic`) and log console (`/api/logs`)
- Node CRUD, single/batch probing, manual blacklist/release
- Subscription management with scheduled refresh and hot reload
- GeoIP region routing (e.g. `/jp`, `/us`) with GeoLite2 auto-update
- Runtime settings persistence (`config.yaml`) and log rotation controls

## Install

```bash
git clone https://github.com/amberpepper/ProxyWeave.git
cd ProxyWeave
cp config.example.yaml config.yaml
touch nodes.txt
```

Edit `config.yaml` and add your nodes (`nodes.txt` / `subscriptions` / `nodes`).

## Start

### Docker (recommended)

```bash
./start.sh
# or
docker compose up -d
```

### Run from source

```bash
go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" ./cmd/proxyweave --config config.yaml
```

## Access

- WebUI: `http://localhost:9091`
- Default proxy (pool): `127.0.0.1:2323`

## Stop

```bash
docker compose down
```
