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
- `GET /ws?userId=&deviceId=&connectionId=&channel=&hostname=&platform=` upgrades a device WebSocket
- `POST /api/device/status`
- `POST /api/device/devices`
- `POST /api/device/message-api`
- `POST /api/device/tool-call`
- `POST /api/device/system-info`
- `POST /api/device/metrics`
- `POST /api/device/rpc`
- `POST /api/device/agent/run`

All `/api/device/*` endpoints require `Authorization: Bearer <SERVICE_TOKEN>` and a JSON body containing `userId`. Device WebSockets also require `userId`; JWT authentication must match that user. Workspace device pools and workspace JWT routing are not supported. Additional `workspaceId` fields do not change device routing.

Tool calls keep one request ID across reconnects, bind to the initially selected device and connection, and allow at most three retransmissions after the first attempt. A call waits for its execution timeout (default 30 seconds, minimum 5 seconds) plus a 45-second recovery window. If no delivery succeeds it returns `503 DEVICE_OFFLINE`; a delivered call without a result returns `504 TIMEOUT`.

Device tunnels (the LobeHub alpha experiment) are not implemented. Tool-call recovery can exceed the generic HTTP `WRITE_TIMEOUT`; reverse proxies must allow the execution timeout plus the 45-second recovery window.

## Device Health Metrics

Authenticated devices send `device_metrics` messages with `batchId` and a `samples` array. The gateway replies with `{ type: "device_metrics_ack", batchId, accepted }`. Each sample contains `connected`, `cpuCount`, `cpuPercent`, `load1`, `load5`, `load15`, `memoryTotalBytes`, `memoryUsedBytes`, and `observedAt` (epoch milliseconds). CPU percentage and load readings may be `null`. Samples may also carry `diskTotalBytes` and `diskUsedBytes` (the volume holding the user's home directory); both are optional; a missing field is omitted on read while `null` is kept as `null`. Samples are stored under the authenticated socket's user and device IDs; IDs supplied in the message cannot redirect them.

`POST /api/device/metrics` accepts `{ userId, deviceId, since? }` with service-token authentication and returns `{ samples: [...] }` in ascending sampling-time order. `deviceId` is required. A numeric `since` is inclusive; absent or non-numeric values default to zero. Results are restricted to the most recent 48 hours, including when the device is offline. Missing history returns `{ samples: [] }`.

The gateway processes the first 500 entries in each batch, validates sample fields, rejects samples older than 48 hours or more than 10 minutes ahead of its clock, and deduplicates by `observedAt`. A later sample at the same timestamp replaces the previous one; duplicates still count as accepted in the ack. Normal devices sample every minute and upload every five minutes. LobeHub's device page normally queries 12 hours of history; aggregation remains on the LobeHub Server.

History is held only in memory, with fixed limits of 3,000 samples per device, 300,000 samples across the gateway, and 1,000 device histories across all users. A device retains its newest samples; global limits evict the entire device history with the oldest latest sample. Expired samples are removed during reads/writes and by a one-minute cleanup timer, even for disconnected devices. Capacity limits may shorten the available history. Restarting the gateway loses all history; subsequent uploads, including any unacknowledged device backlog, repopulate it. No database, disk storage, or external service is required.

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
