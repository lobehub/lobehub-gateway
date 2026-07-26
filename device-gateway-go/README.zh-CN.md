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
- `GET /ws?userId=&workspaceId=&deviceId=&connectionId=&channel=&hostname=&platform=` 升级为设备 WebSocket 连接
- `POST /api/device/status`
- `POST /api/device/devices`
- `POST /api/device/message-api`
- `POST /api/device/tool-call`
- `POST /api/device/system-info`
- `POST /api/device/rpc`
- `POST /api/device/agent/run`

所有 `/api/device/*` 接口都需要 `Authorization: Bearer <SERVICE_TOKEN>`，JSON 请求体中必须包含 `userId` 或 `workspaceId`；两者同时存在时由 `workspaceId` 决定路由范围。包含 `workspace_id` claim 的设备 JWT 用于认证对应 workspace 连接；不包含该 claim 的 JWT 与 API key 用于认证个人用户连接。

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
