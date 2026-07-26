# Go Device Gateway

[简体中文](./README.zh-CN.md)

`device-gateway-go` is a single-instance, self-hosted Go implementation of the LobeHub Device Gateway protocol. It keeps all state in memory and does not depend on Cloudflare Durable Objects, Redis, PostgreSQL, or NATS.

The original Cloudflare Worker Device Gateway remains the reference implementation. This service aims to keep the public HTTP and WebSocket behavior compatible for self-hosted deployments while running as a normal Go service.

## Design

- Single gateway instance with in-memory connection state.
- Platform-neutral HTTP and WebSocket server.
- No Cloudflare Workers, Durable Objects, Redis, PostgreSQL, or NATS dependency.
- Compatible public routes for LobeHub Server, CLI, desktop, and connected devices where practical.

## Endpoints

- `GET /health` returns `OK`
- `GET /ws?userId=&workspaceId=&deviceId=&connectionId=&channel=&hostname=&platform=` upgrades a device WebSocket
- `POST /api/device/status`
- `POST /api/device/devices`
- `POST /api/device/message-api`
- `POST /api/device/tool-call`
- `POST /api/device/system-info`
- `POST /api/device/rpc`
- `POST /api/device/agent/run`

All `/api/device/*` endpoints require `Authorization: Bearer <SERVICE_TOKEN>` and a JSON body containing `userId` or `workspaceId`. When both are present, `workspaceId` selects the routing scope. A device JWT with a `workspace_id` claim authenticates the matching workspace connection; JWTs without it and API keys authenticate personal user connections.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8787` | HTTP listen port. |
| `SERVICE_TOKEN` | required | Shared service token for `/api/device/*` and WebSocket service-token auth. The process refuses to start without it. |
| `JWKS_PUBLIC_KEY` | empty | JWKS JSON containing an RS256 public key for JWT WebSocket auth. |
| `READ_TIMEOUT` | `30s` | Go HTTP server read timeout. |
| `WRITE_TIMEOUT` | `1m` | Go HTTP server write timeout. |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout. |

Protocol-level timeouts intentionally match the Worker implementation: devices must authenticate within 10 seconds and send heartbeats at least every 90 seconds.

## Run locally

```bash
cd device-gateway-go
SERVICE_TOKEN=dev-secret go run ./cmd/device-gateway-go
```

Then configure LobeHub Server with:

```bash
DEVICE_GATEWAY_URL=http://localhost:8787
DEVICE_GATEWAY_SERVICE_TOKEN=dev-secret
```

Devices can connect with:

```bash
lh connect --gateway http://localhost:8787
```

Desktop clients should use the same gateway URL.

## Docker

From the repository root:

```bash
docker build -f device-gateway-go/Dockerfile -t lobehub-device-gateway-go device-gateway-go
docker run --rm -p 8787:8787 -e SERVICE_TOKEN=dev-secret lobehub-device-gateway-go
```

## Reverse proxy notes

If you put the gateway behind Nginx, Caddy, Traefik, or another reverse proxy, WebSocket upgrade support must be enabled for `/ws`.

Example Nginx location:

```nginx
location / {
  proxy_pass http://127.0.0.1:8787;
  proxy_http_version 1.1;
  proxy_set_header Upgrade $http_upgrade;
  proxy_set_header Connection "upgrade";
  proxy_set_header Host $host;
}
```

Example Caddy site:

```caddyfile
gateway.example.com {
  reverse_proxy 127.0.0.1:8787
}
```

Caddy enables WebSocket proxying automatically for normal `reverse_proxy` usage.

## Test

```bash
cd device-gateway-go
go test ./...
```
