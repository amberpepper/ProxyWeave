# ProxyWeave

[English](README.md) | 简体中文

一个基于 sing-box 的代理池管理工具。

## 致谢与项目来源

本项目基于 [jasonwong1991/easy_proxies](https://github.com/jasonwong1991/easy_proxies)
进行二次开发。

原作者： [jasonwong1991](https://github.com/jasonwong1991)

当前维护项目： [amberpepper/ProxyWeave](https://github.com/amberpepper/ProxyWeave)

## 主要特性

- Web 管理面板（`/`）与管理 API（`/api/*`）
- 实时流量带宽图（SSE，`/api/traffic`）与日志控制台（`/api/logs`）
- 节点增删改查、单节点/批量探测、手动拉黑/解封
- 基于 SQLite 持久化设置、节点、订阅和运行时状态
- 每个订阅独立刷新间隔、独立节点关联、独立节点数统计
- GeoIP 分区路由（如 `/jp`、`/us`）及 GeoLite2 自动更新

## 安装

```bash
git clone https://github.com/amberpepper/ProxyWeave.git
cd ProxyWeave
mkdir -p data logs
```

## 启动

### Docker（推荐）

```bash
./start.sh
# 或
docker compose up -d
```

### 直接使用镜像运行

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

如果不使用 host 网络，也可以手动映射端口，例如：

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

### 本地运行

```bash
mkdir -p data logs

go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" ./cmd/proxyweave
```

## 访问

- WebUI：`http://localhost:9091`
- 默认代理入口（pool）：`127.0.0.1:2323`

## 说明

- SQLite 现在是唯一真实数据源。
- 节点、订阅和运行时健康状态会在重启后保留。
- 订阅请在 **节点管理 → 订阅管理** 页面维护。

## 停止

```bash
docker compose down
```
