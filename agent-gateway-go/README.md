# Go Agent Gateway

[简体中文](./README.zh-CN.md)

`agent-gateway-go` is a single-instance, self-hosted Go implementation of the LobeHub Agent Gateway protocol. It relays the browser WebSocket session for an agent conversation and the server-pushed event stream that drives it.

The original Cloudflare Worker Agent Gateway remains the reference implementation. This service aims to keep the public HTTP and WebSocket behavior compatible for self-hosted deployments while running as a normal Go service.

## Design

- Single gateway instance with in-memory operation, event-buffer, and pending-request state.
- Platform-neutral HTTP and WebSocket server.
- No Cloudflare Workers, Durable Objects, Redis, PostgreSQL, or NATS dependency.
- Compatible `/api/operations/*` REST API and `/ws` WebSocket protocol for LobeHub Server and browsers where practical.

## Deliberate trade-offs

- Single process only. The backend `/api/operations/*` calls and the browser WebSocket for the same `operationId` must reach the same instance.
- Runtime state lives in memory. Operations, buffered events, pending requests, metrics and errors are lost on restart.
- The admin surface is intentionally not implemented: `/api/admin/*`, `/admin/ws`, `ADMIN_TOKEN`, admin stats / metrics / errors and admin observer WebSockets are all absent.
- Durable Object storage, alarms and WebSocket hibernation are replaced by in-process maps and timers.

## Endpoints

- `GET /health` returns `OK`
- `GET /ws?operationId={operationId}` upgrades a browser WebSocket
- `POST /api/operations/init`
- `POST /api/operations/push-event`
- `POST /api/operations/push-events`
- `POST /api/operations/update-status`
- `POST /api/operations/request-confirmation`
- `POST /api/operations/request-input`
- `POST /api/operations/tool-execute`
- `GET /api/operations/status?operationId={operationId}`

All `/api/operations/*` endpoints require `Authorization: Bearer <SERVICE_TOKEN>`. The `/ws` endpoint authenticates via the first WebSocket message, which carries either a JWT signed by the LobeHub Server (verified against `JWKS_PUBLIC_KEY`) or the shared service token.

On reconnection, a `resume` message with `"wantStatus": true` receives buffered events followed by `{"type":"resume_complete","status":"..."}` when the operation has an authoritative status. Omitting `wantStatus` preserves replay-only behavior.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8787` | HTTP listen port. |
| `SERVICE_TOKEN` | required | Shared service token for `/api/operations/*` and WebSocket service-token auth. The process refuses to start without it. |
| `JWKS_PUBLIC_KEY` | empty | JWKS JSON containing an RS256 public key for JWT WebSocket auth. |
| `LOBE_API_BASE_URL` | `https://app.lobehub.com` | LobeHub API base URL used for backend callbacks such as abandoned-operation reconciliation and tool-result forwarding. Point this at your own LobeHub instance for self-hosted deployments. |
| `READ_TIMEOUT` | `30s` | Go HTTP server read timeout. |
| `WRITE_TIMEOUT` | `1m` | Go HTTP server write timeout. |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout. |

## Run locally

```bash
cd agent-gateway-go
SERVICE_TOKEN=dev-secret go run ./cmd/agent-gateway-go
```

Then configure LobeHub Server with:

```bash
AGENT_GATEWAY_URL=http://localhost:8787
AGENT_GATEWAY_SERVICE_TOKEN=dev-secret
```

`AGENT_GATEWAY_URL` is consumed by **both** the LobeHub Server (as a server-to-server fetch target for `/api/operations/*`) and the browser (as the WebSocket origin). In a self-hosted deployment behind a reverse proxy, set it to the public URL the browser can reach — the server will route through the same URL.

`JWKS_PUBLIC_KEY` must contain the public half of the same RSA key that LobeHub Server uses to sign internal JWTs (`JWKS_KEY` on the server). The gateway only reads the `kty`, `alg`, `n`, and `e` fields, so it is safe to pass the same JWKS JSON that contains the private key — the private fields are ignored.

## When does this gateway get used?

Once `AGENT_GATEWAY_URL` is set on the LobeHub Server, two paths route through this gateway:

1. **Gateway Mode** — User toggles _Settings → Advanced → Labs → Gateway Mode_. After that, the regular chat loop is executed server-side by `AgentRuntime` instead of in the browser; events are pushed through `agent-gateway-go` over WebSocket.
2. **Heterogeneous agents on web** (Claude Code, Codex, …) — These always run via the gateway on web, independent of the Gateway Mode toggle, because the cloud sandbox is their only execution environment.

The `enableGatewayMode` lab switch is hidden in the LobeHub UI until `AGENT_GATEWAY_URL` is configured on the server.

## Reverse proxy notes

If you put the gateway behind Nginx, Caddy, Traefik or another reverse proxy, WebSocket upgrade support must be enabled for `/ws`. The browser opens a long-lived WebSocket that sends heartbeats while an agent operation is running, so do not set short read/send timeouts on the proxy.

Subdomain-style Nginx server (gateway on its own hostname):

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

Path-prefix Nginx location (gateway sharing the LobeHub Server hostname). Note the trailing slash on `proxy_pass` so the prefix is stripped — the gateway must see `/ws` and `/api/operations/*` at the root:

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

With path-prefix routing, set `AGENT_GATEWAY_URL=https://your-lobehub-host/agent-gateway` on the LobeHub Server.

Caddy site:

```caddyfile
agent-gateway.example.com {
  reverse_proxy 127.0.0.1:8787
}
```

Caddy enables WebSocket proxying automatically for normal `reverse_proxy` usage.

## Docker Compose

Cross-compile the binary first:

```bash
cd agent-gateway-go
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o agent-gateway-go-linux-amd64 ./cmd/agent-gateway-go
```

Then mount it into a minimal Alpine container alongside LobeHub. The example below assumes a `device-gateway` service is already running on host port `8787`, so the agent gateway takes host port `8788`:

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

Matching `.env` entries for the LobeHub Server (alongside any existing `JWKS_KEY`):

```bash
AGENT_GATEWAY_URL=https://your-lobehub-host/agent-gateway
AGENT_GATEWAY_SERVICE_TOKEN=<openssl rand -base64 32>
```

When you change these values, recreate (not just restart) the LobeHub container so the new environment is picked up:

```bash
docker compose up -d --force-recreate lobe
```

## Test

```bash
cd agent-gateway-go
go test ./...
```
