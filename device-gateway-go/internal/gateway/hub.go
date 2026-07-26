package gateway

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var errPrincipalMismatch = errors.New("principal mismatch")

type pendingRequest struct {
	resolve func(rpcEnvelope)
	timer   *time.Timer
}

type dispatchStatus int

const (
	dispatchOK dispatchStatus = iota
	dispatchTimeout
	dispatchOffline
)

type hub struct {
	connections map[string]*connection
	pending     map[string]pendingRequest
	mu          sync.RWMutex
	principal   string
}

func newHub(principal string) *hub {
	return &hub{
		connections: map[string]*connection{},
		pending:     map[string]pendingRequest{},
		principal:   principal,
	}
}

func (h *hub) register(conn *connection) {
	h.mu.Lock()
	old := h.connections[conn.att.ConnectionID]
	h.connections[conn.att.ConnectionID] = conn
	h.mu.Unlock()

	if old != nil && old != conn {
		old.close(wsCloseNormal, "Replaced by new connection")
	}
}

func (h *hub) remove(conn *connection) {
	h.mu.Lock()
	if current := h.connections[conn.att.ConnectionID]; current == conn {
		delete(h.connections, conn.att.ConnectionID)
	}
	h.mu.Unlock()
	if conn.heartbeatTimer != nil {
		conn.heartbeatTimer.Stop()
	}
}

func (h *hub) markAuthenticated(conn *connection) {
	h.mu.Lock()
	conn.att.Authenticated = true
	conn.att.LastHeartbeat = time.Now().UnixMilli()
	h.mu.Unlock()
}

func (h *hub) recordHeartbeat(conn *connection) {
	h.mu.Lock()
	conn.att.LastHeartbeat = time.Now().UnixMilli()
	h.mu.Unlock()
}

func (h *hub) isAuthenticated(conn *connection) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return conn.att.Authenticated
}

func (h *hub) authenticatedConnections() []*connection {
	h.mu.RLock()
	defer h.mu.RUnlock()
	connections := make([]*connection, 0, len(h.connections))
	for _, conn := range h.connections {
		if conn.att.Authenticated {
			connections = append(connections, conn)
		}
	}
	return connections
}

func (h *hub) deviceCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	deviceIDs := map[string]struct{}{}
	for _, conn := range h.connections {
		if conn.att.Authenticated {
			deviceIDs[conn.att.DeviceID] = struct{}{}
		}
	}
	return len(deviceIDs)
}

func (h *hub) devices() []GatewayDevice {
	h.mu.RLock()
	defer h.mu.RUnlock()
	byDevice := map[string]*GatewayDevice{}
	for _, conn := range h.connections {
		if !conn.att.Authenticated {
			continue
		}
		channel := DeviceConnection{
			Channel:      conn.att.Channel,
			ConnectedAt:  conn.att.ConnectedAt,
			ConnectionID: conn.att.ConnectionID,
		}
		device := byDevice[conn.att.DeviceID]
		if device == nil {
			byDevice[conn.att.DeviceID] = &GatewayDevice{
				Channels:    []DeviceConnection{channel},
				ConnectedAt: conn.att.ConnectedAt,
				DeviceID:    conn.att.DeviceID,
				Hostname:    conn.att.Hostname,
				Platform:    conn.att.Platform,
			}
			continue
		}
		device.Channels = append(device.Channels, channel)
		if conn.att.ConnectedAt > device.ConnectedAt {
			device.ConnectedAt = conn.att.ConnectedAt
			device.Hostname = conn.att.Hostname
			device.Platform = conn.att.Platform
		}
	}
	devices := make([]GatewayDevice, 0, len(byDevice))
	for _, device := range byDevice {
		sort.SliceStable(device.Channels, func(i, j int) bool {
			return device.Channels[i].ConnectedAt > device.Channels[j].ConnectedAt
		})
		devices = append(devices, *device)
	}
	sort.SliceStable(devices, func(i, j int) bool {
		return devices[i].ConnectedAt > devices[j].ConnectedAt
	})
	return devices
}

func (h *hub) target(deviceID string) *connection {
	connections := h.authenticatedConnections()
	candidates := make([]*connection, 0, len(connections))
	for _, conn := range connections {
		if deviceID == "" || conn.att.DeviceID == deviceID {
			candidates = append(candidates, conn)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return byDispatchPriority(candidates[i].att, candidates[j].att) < 0
	})
	return candidates[0]
}

func byDispatchPriority(a DeviceAttachment, b DeviceAttachment) int {
	if byChannel := channelRank(a.Channel) - channelRank(b.Channel); byChannel != 0 {
		return byChannel
	}
	if a.ConnectedAt > b.ConnectedAt {
		return -1
	}
	if a.ConnectedAt < b.ConnectedAt {
		return 1
	}
	return 0
}

func channelRank(channel string) int {
	switch channel {
	case "cli":
		return 0
	case "cli-dev":
		return 1
	case "desktop":
		return 2
	case "desktop-dev":
		return 3
	default:
		return 4
	}
}

func (h *hub) setPending(key string, timeout time.Duration, resolve func(rpcEnvelope), onTimeout func()) {
	timer := time.AfterFunc(timeout, func() {
		h.mu.Lock()
		delete(h.pending, key)
		h.mu.Unlock()
		onTimeout()
	})
	h.mu.Lock()
	h.pending[key] = pendingRequest{resolve: resolve, timer: timer}
	h.mu.Unlock()
}

func (h *hub) clearPending(key string) {
	h.mu.Lock()
	pending, ok := h.pending[key]
	if ok {
		delete(h.pending, key)
	}
	h.mu.Unlock()
	if ok {
		pending.timer.Stop()
	}
}

func (h *hub) resolvePending(msg rpcEnvelope) {
	key := msg.RequestID
	if key == "" {
		key = msg.OperationID
	}
	if key == "" {
		return
	}
	h.mu.Lock()
	pending, ok := h.pending[key]
	if ok {
		delete(h.pending, key)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	pending.timer.Stop()
	pending.resolve(msg)
}

func (h *hub) dispatch(target *connection, key string, timeout time.Duration, payload map[string]any) (rpcEnvelope, dispatchStatus) {
	resultCh := make(chan rpcEnvelope, 1)
	timeoutCh := make(chan struct{}, 1)
	h.setPending(key, timeout, func(msg rpcEnvelope) { resultCh <- msg }, func() { timeoutCh <- struct{}{} })
	if err := target.writeJSON(payload); err != nil {
		h.clearPending(key)
		return rpcEnvelope{}, dispatchOffline
	}
	select {
	case msg := <-resultCh:
		return msg, dispatchOK
	case <-timeoutCh:
		return rpcEnvelope{}, dispatchTimeout
	}
}
