# Go Agent Gateway

[English](./README.md)

`agent-gateway-go` 是 LobeHub Agent Gateway 协议的单实例、自托管 Go 实现。它中转 agent 会话的浏览器 WebSocket 连接，以及服务端推送的 agent 事件流。

原始 Cloudflare Worker Agent Gateway 仍然是参考实现。本服务的目标是在自托管部署中尽量保持公开 HTTP 与 WebSocket 行为兼容，同时以普通 Go 服务的形式运行。

## 设计

- 单网关实例，operation、事件缓冲、pending 请求等状态保存在内存中。
- 平台无关的 HTTP 与 WebSocket 服务。
- 不依赖 Cloudflare Workers、Durable Objects、Redis、PostgreSQL 或 NATS。
- 在可行范围内保持面向 LobeHub Server 和浏览器的 `/api/operations/*` REST API 与 `/ws` WebSocket 协议兼容。

## 明确取舍

- 只支持单进程。相同 `operationId` 的后端 `/api/operations/*` 请求和浏览器 WebSocket 必须到达同一个实例。
- 运行时状态保存在内存中。重启后 operation、事件缓冲、pending 请求、metrics 和 errors 都会丢失。
- 不实现 admin 模块。包括 `/api/admin/*`、`/admin/ws`、`ADMIN_TOKEN`、admin stats、admin metrics、admin errors 和 admin observer WebSocket。
- Durable Object storage、alarm 和 WebSocket hibernation 使用进程内 map 与 timer 替代。

## 接口

- `GET /health` 返回 `OK`
- `GET /ws?operationId={operationId}` 升级为浏览器 WebSocket 连接
- `POST /api/operations/init`
- `POST /api/operations/push-event`
- `POST /api/operations/push-events`
- `POST /api/operations/update-status`
- `POST /api/operations/request-confirmation`
- `POST /api/operations/request-input`
- `POST /api/operations/tool-execute`
- `GET /api/operations/status?operationId={operationId}`

所有 `/api/operations/*` 接口都要求 `Authorization: Bearer <SERVICE_TOKEN>`。`/ws` 通过首条 WebSocket 消息认证，消息可携带 LobeHub Server 签发的 JWT（用 `JWKS_PUBLIC_KEY` 校验），或携带共享的 service token。

重新连接时，携带 `"wantStatus": true` 的 `resume` 消息会先收到缓冲事件；若 operation 已有权威状态，随后还会收到 `{"type":"resume_complete","status":"..."}`。省略 `wantStatus` 时保持仅重放事件的兼容行为。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8787` | HTTP 监听端口。 |
| `SERVICE_TOKEN` | 必填 | `/api/operations/*` 与 WebSocket service-token 认证共用的服务令牌。未设置时进程会拒绝启动。 |
| `JWKS_PUBLIC_KEY` | 空 | 包含 RS256 公钥的 JWKS JSON,用于 WebSocket JWT 认证。 |
| `LOBE_API_BASE_URL` | `https://app.lobehub.com` | 用于 abandoned operation reconciliation、tool-result forwarding 等后端回调的 LobeHub API 基础地址。自托管部署需指向你自己的 LobeHub 实例。 |
| `READ_TIMEOUT` | `30s` | Go HTTP 服务器读取超时时间。 |
| `WRITE_TIMEOUT` | `1m` | Go HTTP 服务器写入超时时间。 |
| `SHUTDOWN_TIMEOUT` | `10s` | 优雅关闭超时时间。 |

## 本地运行

```bash
cd agent-gateway-go
SERVICE_TOKEN=dev-secret go run ./cmd/agent-gateway-go
```

然后为 LobeHub Server 配置：

```bash
AGENT_GATEWAY_URL=http://localhost:8787
AGENT_GATEWAY_SERVICE_TOKEN=dev-secret
```

`AGENT_GATEWAY_URL` 同时被 **服务端**（用于向 `/api/operations/*` 发起 server-to-server fetch）和 **浏览器**（用作 WebSocket origin）消费。自托管部署在反向代理后面时，请将该值设为浏览器可以访问的公网 URL —— 服务端也会走同一个 URL。

`JWKS_PUBLIC_KEY` 必须是 LobeHub Server 签发内部 JWT 所用同一组 RSA 密钥的公钥（服务端的环境变量名是 `JWKS_KEY`）。网关只读取 `kty`、`alg`、`n`、`e` 字段，因此把包含私钥的完整 JWKS JSON 直接传入也是安全的 —— 私钥字段会被忽略。

## 这个网关在什么情况下会被用到

当 LobeHub Server 上设置了 `AGENT_GATEWAY_URL`,有两条路径会走到这个网关：

1. **Gateway Mode**:用户在 _设置 → 高级 → 实验性功能 → Gateway Mode_ 打开开关。之后普通对话循环由服务端的 `AgentRuntime` 执行（而不是在浏览器内运行）,事件经 `agent-gateway-go` 用 WebSocket 推回浏览器。
2. **Web 端的异构 agent**(Claude Code、Codex 等):在 web 端总是走 gateway,与 Gateway Mode 开关无关 —— 因为云端 sandbox 是它们唯一的执行环境。

在 LobeHub UI 中,`enableGatewayMode` 这个 lab 开关只有当服务端配置了 `AGENT_GATEWAY_URL` 时才会渲染。

## 反向代理说明

如果将 gateway 放在 Nginx、Caddy、Traefik 或其他反向代理后面,必须为 `/ws` 启用 WebSocket upgrade 支持。浏览器在 agent operation 进行期间会保持一条带心跳的长连接,因此不要在代理上设置过短的读写超时。

子域名形式的 Nginx server（网关单独使用一个 hostname）：

```nginx
server {
  listen 443 ssl http2;
  server_name agent-gateway.example.com;

  location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $http_host;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
  }
}
```

路径前缀形式的 Nginx location（网关与 LobeHub Server 共用同一个 hostname）。注意 `proxy_pass` 末尾的斜杠 —— 它会把前缀剥掉,网关需要看到根路径下的 `/ws` 和 `/api/operations/*`：

```nginx
location /agent-gateway/ {
  proxy_pass http://127.0.0.1:8787/;
  proxy_http_version 1.1;
  proxy_set_header Upgrade $http_upgrade;
  proxy_set_header Connection "upgrade";
  proxy_set_header Host $http_host;
  proxy_read_timeout 3600s;
  proxy_send_timeout 3600s;
}
```

使用路径前缀时,LobeHub Server 上的 `AGENT_GATEWAY_URL` 要设为 `https://your-lobehub-host/agent-gateway`。

Caddy site：

```caddyfile
agent-gateway.example.com {
  reverse_proxy 127.0.0.1:8787
}
```

Caddy 在普通 `reverse_proxy` 用法下会自动启用 WebSocket 代理。

## Docker Compose

先交叉编译二进制：

```bash
cd agent-gateway-go
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o agent-gateway-go-linux-amd64 ./cmd/agent-gateway-go
```

然后把它挂载进一个最小 Alpine 容器,与 LobeHub 并排运行。下面的示例假设已经有一个 `device-gateway` 占用了宿主 `8787`,因此 agent gateway 用 `8788`：

```yaml
services:
  lobe:
    image: lobehub/lobehub
    container_name: lobe-chat
    ports:
      - "3211:3210"
    env_file: [.env]
    restart: always
    depends_on: [agent-gateway]

  agent-gateway:
    image: alpine:3.20
    container_name: agent-gateway
    ports:
      - "8788:8787"
    volumes:
      - ./agent-gateway-go-linux-amd64:/usr/local/bin/agent-gateway-go:ro
    entrypoint: ["/usr/local/bin/agent-gateway-go"]
    environment:
      SERVICE_TOKEN: ${AGENT_GATEWAY_SERVICE_TOKEN}
      JWKS_PUBLIC_KEY: ${JWKS_KEY}
      LOBE_API_BASE_URL: https://your-lobehub-host
    restart: always
```

LobeHub Server 的 `.env` 需要追加（与已有的 `JWKS_KEY` 并存）：

```bash
AGENT_GATEWAY_URL=https://your-lobehub-host/agent-gateway
AGENT_GATEWAY_SERVICE_TOKEN=<openssl rand -base64 32>
```

修改这两个变量后,要 **重建**（不是 restart）LobeHub 容器才能读到新环境变量：

```bash
docker compose up -d --force-recreate lobe
```

## 测试

```bash
cd agent-gateway-go
go test ./...
```
