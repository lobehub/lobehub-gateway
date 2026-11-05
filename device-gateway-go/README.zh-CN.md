# Go Device Gateway

[English](./README.md)

`device-gateway-go` 是 LobeHub Device Gateway 协议的单实例、自托管 Go 实现。它将所有状态保存在内存中，不依赖 Cloudflare Durable Objects、Redis、PostgreSQL 或 NATS。

原始 Cloudflare Worker Device Gateway 仍然是参考实现。本服务的目标是在自托管部署中尽量保持公开 HTTP 与 WebSocket 行为兼容，同时以普通 Go 服务的形式运行。

## 设计

- 单网关实例，连接状态保存在内存中。
- 平台无关的 HTTP 与 WebSocket 服务。
- 不依赖 Cloudflare Workers、Durable Objects、Redis、PostgreSQL 或 NATS。
- 在可行范围内保持面向 LobeHub Server、CLI、桌面端和连接设备的公开路由兼容。

## 接口

- `GET /health` 返回 `OK`
- `GET /ws?userId=&deviceId=&connectionId=&channel=&hostname=&platform=` 升级为设备 WebSocket 连接
- `POST /api/device/status`
- `POST /api/device/devices`
- `POST /api/device/message-api`
- `POST /api/device/tool-call`
- `POST /api/device/system-info`
- `POST /api/device/metrics`
- `POST /api/device/rpc`
- `POST /api/device/agent/run`

所有 `/api/device/*` 接口都需要 `Authorization: Bearer <SERVICE_TOKEN>`，JSON 请求体必须包含 `userId`。设备 WebSocket 同样需要 `userId`，JWT 认证必须与该用户一致。不支持 Workspace 设备池及工作区 JWT 路由，额外的 `workspaceId` 字段不会改变设备路由。

工具调用在重连重发时保留同一 request ID，并绑定首次选定的设备与连接。首次发送后最多重发三次。等待时间为执行超时（默认 30 秒，最少 5 秒）加 45 秒恢复窗口。始终未成功投递时返回 `503 DEVICE_OFFLINE`；成功投递但未收到结果时返回 `504 TIMEOUT`。

不实现设备隧道（LobeHub 的 alpha 实验功能）。工具调用恢复的等待时间可以超出通用 HTTP `WRITE_TIMEOUT`，反向代理需允许执行超时加 45 秒恢复窗口。

## 设备健康指标

已认证的设备发送 `device_metrics` 消息，包含 `batchId` 和 `samples` 数组；网关回复 `{ type: "device_metrics_ack", batchId, accepted }`。样本字段为 `connected`、`cpuCount`、`cpuPercent`、`load1`、`load5`、`load15`、`memoryTotalBytes`、`memoryUsedBytes` 和 `observedAt`（毫秒时间戳），CPU 使用率和负载允许为 `null`。样本还可带上 `diskTotalBytes` 和 `diskUsedBytes`（用户主目录所在磁盘），两者均可选：缺失的字段在读取时省略，显式 `null` 原样返回。样本归属由已认证 socket 的用户和设备 ID 决定，消息中额外提供的 ID 不会改变归属。

`POST /api/device/metrics` 使用 Service Token 认证，接受 `{ userId, deviceId, since? }`，返回按采样时间升序排列的 `{ samples: [...] }`。`deviceId` 必填，数字 `since` 为包含边界的查询起点，缺省或非数字时按零处理。结果限制为最近 48 小时，设备离线后仍可查询；没有历史时返回 `{ samples: [] }`。

单批仅处理前 500 条，校验样本字段，丢弃早于 48 小时或超前网关时间超过 10 分钟的样本，并按 `observedAt` 去重。同一时间戳的新样本替换旧样本，重复样本仍计入 ack 的接受数量。正常设备每分钟采样、每五分钟上传；LobeHub 设备页面通常查询最近 12 小时，图表聚合由 LobeHub Server 完成。

历史仅保存在内存中，固定上限为每设备 3,000 条、整个网关 300,000 条、所有用户合计 1,000 个设备历史。单设备保留最新样本；触及全局上限时，整体淘汰最新样本时间最早的设备历史。读写时及每分钟定时清理过期样本，断线设备也会清理。容量限制可能缩短可查询的历史范围。网关重启后全部历史丢失，后续上传及设备尚未确认的积压样本会重新填充历史。不需要数据库、磁盘存储或外部服务。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8787` | HTTP 监听端口。 |
| `SERVICE_TOKEN` | 必填 | `/api/device/*` 与 WebSocket service-token 认证共用的服务令牌。未设置时进程会拒绝启动。 |
| `JWKS_PUBLIC_KEY` | 空 | 包含 RS256 公钥的 JWKS JSON，用于 WebSocket JWT 认证。 |
| `READ_TIMEOUT` | `30s` | Go HTTP 服务器读取超时时间。 |
| `WRITE_TIMEOUT` | `1m` | Go HTTP 服务器写入超时时间。 |
| `SHUTDOWN_TIMEOUT` | `10s` | 优雅关闭超时时间。 |

协议级超时刻意与 Worker 实现保持一致：设备必须在 10 秒内完成认证，并且至少每 90 秒发送一次心跳。

## 本地运行

```bash
cd device-gateway-go
SERVICE_TOKEN=dev-secret go run ./cmd/device-gateway-go
```

然后为 LobeHub Server 配置：

```bash
DEVICE_GATEWAY_URL=http://localhost:8787
DEVICE_GATEWAY_SERVICE_TOKEN=dev-secret
```

设备可以通过以下方式连接：

```bash
lh connect --gateway http://localhost:8787
```

桌面客户端也应使用相同的 gateway URL。

## Docker

从仓库根目录运行：

```bash
docker build -f device-gateway-go/Dockerfile -t lobehub-device-gateway-go device-gateway-go
docker run --rm -p 8787:8787 -e SERVICE_TOKEN=dev-secret lobehub-device-gateway-go
```

## 反向代理说明

如果将 gateway 放在 Nginx、Caddy、Traefik 或其他反向代理后面，必须为 `/ws` 启用 WebSocket upgrade 支持。

Nginx location 示例：

```nginx
location / {
  proxy_pass http://127.0.0.1:8787;
  proxy_http_version 1.1;
  proxy_set_header Upgrade $http_upgrade;
  proxy_set_header Connection "upgrade";
  proxy_set_header Host $host;
}
```

Caddy site 示例：

```caddyfile
gateway.example.com {
  reverse_proxy 127.0.0.1:8787
}
```

Caddy 在普通 `reverse_proxy` 用法下会自动启用 WebSocket 代理。

## 测试

```bash
cd device-gateway-go
go test ./...
```
