# ProxyWeave

[English](README.md) | 简体中文

一个基于 sing-box 的代理池管理工具。

## 致谢与项目来源

本项目基于 [jasonwong1991/easy_proxies](https://github.com/jasonwong1991/easy_proxies)
进行二次开发。

原作者： [jasonwong1991](https://github.com/jasonwong1991)

当前维护项目： [amberpepper/ProxyWeave](https://github.com/amberpepper/ProxyWeave)

## 主要特性

当前版本主要包含：

- Web 管理面板（`/`）与管理 API（`/api/*`）
- 实时流量带宽图（SSE，`/api/traffic`）与日志控制台（`/api/logs`）
- 节点配置管理（增删改查）、单节点探测、批量探测、手动拉黑/解封
- 订阅管理与定时刷新，刷新后热重载
- GeoIP 分区路由（如 `/jp`、`/us`）及 GeoLite2 自动更新
- 运行时设置保存（写回 `config.yaml`）、日志轮转配置

## 安装

```bash
git clone https://github.com/amberpepper/ProxyWeave.git
cd ProxyWeave
cp config.example.yaml config.yaml
touch nodes.txt
```

编辑 `config.yaml`，并配置节点来源（`nodes.txt` / `subscriptions` / `nodes`）。

## 启动

### Docker（推荐）

```bash
./start.sh
# 或
docker compose up -d
```

### 本地运行

```bash
go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" ./cmd/proxyweave -config config.yaml
```

## 访问

- WebUI：`http://localhost:9091`
- 默认代理入口（pool）：`127.0.0.1:2323`

## 停止

```bash
docker compose down
```
