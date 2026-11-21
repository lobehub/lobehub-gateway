# Go Agent Gateway

[简体中文](./README.zh-CN.md)

`agent-gateway-go` is a single-instance, self-hosted Go implementation of the LobeHub Agent Gateway protocol. It relays the browser WebSocket session for an agent conversation and the server-pushed event stream that drives it.

The original Cloudflare Worker Agent Gateway remains the reference implementation. This service aims to keep the public HTTP and WebSocket behavior compatible for self-hosted deployments while running as a normal Go service.

## Design

- Single gateway instance with in-memory operation, event-buffer, pending-request, and v2 user-hub state.
- Platform-neutral HTTP and WebSocket server.
- No Cloudflare Workers, Durable Objects, Redis, PostgreSQL, or NATS dependency.
- Compatible `/api/operations/*` REST API and both `/ws` (v1) and `/v2/ws` (multiplexed) WebSocket protocols for LobeHub Server and browsers where practical.

## Deliberate trade-offs

- Single process only. The backend `/api/operations/*` calls and the browser WebSocket for the same `operationId` must reach the same instance.
- Runtime state lives in memory. Operations, buffered events, pending requests, v2 subscriptions and open LLM attempts are lost on restart. V2 clients reconnect and resubscribe, but events buffered only before a process restart cannot be replayed and must be recovered from LobeHub storage.
- The admin surface is intentionally not implemented: `/api/admin/*`, `/admin/ws`, `ADMIN_TOKEN`, admin stats / metrics / errors and admin observer WebSockets are all absent.
- Durable Object storage, alarms and WebSocket hibernation are replaced by in-process maps and timers.

## Endpoints

- `GET /health` returns `OK`
- `GET /ws?operationId={operationId}` upgrades a browser WebSocket
- `GET /v2/ws?token={jwt}&clientId={clientId}` upgrades a user-level multiplexed browser WebSocket
- `POST /api/operations/init`
- `POST /api/operations/push-event`
- `POST /api/operations/push-events`
- `POST /api/operations/update-status`
- `POST /api/operations/request-confirmation`
- `POST /api/operations/request-input`
- `POST /api/operations/tool-execute`
- `POST /api/operations/llm-execute`
- `POST /api/operations/llm-cancel`
- `POST /api/operations/llm-close`
- `GET /api/operations/status?operationId={operationId}`

All `/api/operations/*` endpoints require `Authorization: Bearer <SERVICE_TOKEN>`. The `/ws` endpoint authenticates via the first WebSocket message, which carries either a JWT signed by the LobeHub Server (verified against `JWKS_PUBLIC_KEY`) or the shared service token.

Protocol v2 authenticates during the `/v2/ws` upgrade. `token` is required; `tokenType` defaults to `jwt` and may be `apiKey` when `serverUrl` is also supplied. Authentication failures complete the WebSocket upgrade and immediately close it with code `4401` and reason `auth_expired` or `auth_failed`, allowing browser clients to distinguish refreshable credentials. A successful connection receives:

```json
{ "type": "ready", "userId": "user-id", "connectionId": "...", "protocol": 2 }
```

The connection multiplexes operations with `subscribe` and `unsubscribe`. Stream messages carry the subscribed `operationId`; replay ends with `resume_complete`, including `gap: true` when the in-memory buffer no longer contains a contiguous history. `executor: true` marks the subscription that should receive `tool_execute` events; if no executor exists, all subscribers receive them for v1 parity. Client `tool_result`, `tool_confirmation`, `user_input`, and `interrupt` messages must target an operation subscribed on the same connection.

`POST /api/operations/init` accepts optional metadata fields `topicId`, `threadId`, `agentId`, `groupId`, `taskId`, `scope`, `parentOperationId`, `mirrorToOperationId`, `rootOperationId`, and `heteroType`. V2 connections receive user-level `op_lifecycle` messages containing this metadata and the precise operation status. Unknown metadata fields are ignored.

`llm-execute` accepts `{ operationId, data: { callId, preferredClientId?, deadlines?: { totalMs? }, ... } }` and forwards opaque attempt data as an `agent_event` of type `llm_execute`. V1 clients identify themselves with `clientId` in their `auth` message; v2 clients use the connection's `clientId` query parameter. Delivery prefers the starter identified by `preferredClientId`. If absent, v2 prefers executor subscriptions, then all subscribers; v1 falls back to all authenticated clients. The response reports `{ success: true, delivered }`; operations whose run has ended return `409 { delivered: 0, error: "OPERATION_ENDED" }`. An operation whose session ended on a mirrored terminal accepts it and reopens, as described below.

`llm-cancel` accepts `{ operationId, data: { callId, ... } }` and broadcasts `llm_cancel` to every client. `llm-close` accepts `{ operationId, callId }` and retires the attempt without broadcasting. Resume replays only open, unexpired attempts, including attempts trimmed out of the event ring, with the same recipient selection as live delivery. The default replay lifetime is 10 minutes, overridden by a positive `data.deadlines.totalMs`. Cancellation, close and terminal operation status stop execution replay; open calls are lost on process restart.

The inactivity watchdog starts at operation initialization. Every backend activity refreshes a fixed 30-minute window, or 60 minutes when `init.meta.heteroType` is nonempty. Event types do not change the window. When the window elapses, the gateway calls `POST /api/agent/finalize-abandoned` on `LOBE_API_BASE_URL` with reason `inactivity_watchdog`.

Every `agent_runtime_end` ends the session: clients receive `session_complete`. An end naming the operation itself (or no operation) also ends its run, and the operation is removed from memory 10 minutes later. An end naming another operation is a terminal mirrored onto this channel, such as a group member's on its supervisor's channel, and the channel's own run continues, so the operation stays in memory under the watchdog. A later `step_start`, `stream_start` or `tool_start` event, `tool-execute`, `llm-execute` or non-terminal `update-status` returns it to `running`. Other events refresh the idle window but never reopen it. If the window elapses first, the gateway calls `finalize-abandoned` without sending clients a second terminal, then removes the operation 10 minutes later.

If LobeHub does not acknowledge the watchdog's call (network error, non-2xx response or unreadable body), the gateway logs a warning and retries once, with reason `gateway_cleanup`, before the operation is removed from memory. A failed retry is logged as an error. Operations whose run ended, or whose watchdog call was acknowledged, are never finalized again. Watchdog state is held in memory, so operations that are running when the gateway restarts are not finalized by the gateway; LobeHub's stale-operation sweep retires them.

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

If you put the gateway behind Nginx, Caddy, Traefik or another reverse proxy, WebSocket upgrade support must be enabled for both `/ws` and `/v2/ws`. The browser opens a long-lived WebSocket that sends heartbeats while agent operations are running, so do not set short read/send timeouts on the proxy.

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
