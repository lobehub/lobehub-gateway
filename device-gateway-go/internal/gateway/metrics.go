package gateway

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	metricRetention       = 48 * time.Hour
	metricFutureSkew      = 10 * time.Minute
	metricMaxBatch        = 500
	metricMaxPerDevice    = 3000
	metricMaxTotal        = 300000
	metricMaxDevices      = 1000
	metricCleanupInterval = time.Minute
)

// Disk readings stay raw so absent fields marshal as omitted while explicit
// null reads back as null.
type metricSample struct {
	Connected        bool            `json:"connected"`
	CPUCount         float64         `json:"cpuCount"`
	CPUPercent       *float64        `json:"cpuPercent"`
	DiskTotalBytes   json.RawMessage `json:"diskTotalBytes,omitempty"`
	DiskUsedBytes    json.RawMessage `json:"diskUsedBytes,omitempty"`
	Load1            *float64        `json:"load1"`
	Load15           *float64        `json:"load15"`
	Load5            *float64        `json:"load5"`
	MemoryTotalBytes float64         `json:"memoryTotalBytes"`
	MemoryUsedBytes  float64         `json:"memoryUsedBytes"`
	ObservedAt       float64         `json:"observedAt"`
}

type metricKey struct{ userID, deviceID string }

type metricStore struct {
	mu              sync.Mutex
	series          map[metricKey][]metricSample
	total           int
	timer           *time.Timer
	cleanupInterval time.Duration
}

func newMetricStore() *metricStore {
	return &metricStore{series: make(map[metricKey][]metricSample), cleanupInterval: metricCleanupInterval}
}

func decodeMetricSample(raw json.RawMessage) (metricSample, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return metricSample{}, false
	}
	// Required numeric/boolean fields cannot be missing or null. Nullable fields
	// must also be present, so a missing reading is serialized explicitly as null.
	for _, name := range []string{"connected", "cpuCount", "memoryTotalBytes", "memoryUsedBytes", "observedAt"} {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return metricSample{}, false
		}
	}
	for _, name := range []string{"cpuPercent", "load1", "load5", "load15"} {
		if _, ok := fields[name]; !ok {
			return metricSample{}, false
		}
	}
	var sample metricSample
	if json.Unmarshal(raw, &sample) != nil {
		return metricSample{}, false
	}
	for _, value := range []float64{sample.CPUCount, sample.MemoryTotalBytes, sample.MemoryUsedBytes, sample.ObservedAt} {
		if !metricNumber(value) {
			return metricSample{}, false
		}
	}
	for _, raw := range []json.RawMessage{sample.DiskTotalBytes, sample.DiskUsedBytes} {
		if len(raw) == 0 {
			continue
		}
		var value *float64
		if json.Unmarshal(raw, &value) != nil || value != nil && !metricNumber(*value) {
			return metricSample{}, false
		}
	}
	for _, value := range []*float64{sample.CPUPercent, sample.Load1, sample.Load5, sample.Load15} {
		if value != nil && !metricNumber(*value) {
			return metricSample{}, false
		}
	}
	if sample.CPUPercent != nil && *sample.CPUPercent > 100 {
		return metricSample{}, false
	}
	return sample, true
}

func metricNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

// Accepted counts valid samples (including duplicates), as in the upstream ack.
// Retention is best effort under capacity limits; the newest data takes priority.
func (s *metricStore) store(key metricKey, raw []json.RawMessage, now time.Time) int {
	raw = raw[:min(len(raw), metricMaxBatch)]
	oldest, newest := float64(now.Add(-metricRetention).UnixMilli()), float64(now.Add(metricFutureSkew).UnixMilli())
	incoming := make([]metricSample, 0, len(raw))
	for _, item := range raw {
		sample, ok := decodeMetricSample(item)
		if ok && sample.ObservedAt >= oldest && sample.ObservedAt <= newest {
			incoming = append(incoming, sample)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	if len(incoming) > 0 {
		existing := s.series[key]
		merged := make(map[float64]metricSample, len(existing)+len(incoming))
		for _, sample := range existing {
			merged[sample.ObservedAt] = sample
		}
		for _, sample := range incoming {
			merged[sample.ObservedAt] = sample
		}
		samples := make([]metricSample, 0, len(merged))
		for _, sample := range merged {
			samples = append(samples, sample)
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i].ObservedAt < samples[j].ObservedAt })
		if len(samples) > metricMaxPerDevice {
			samples = append([]metricSample(nil), samples[len(samples)-metricMaxPerDevice:]...)
		}
		s.total += len(samples) - len(existing)
		s.series[key] = samples
		for s.total > metricMaxTotal || len(s.series) > metricMaxDevices {
			s.evictOldestLocked()
		}
	}
	s.scheduleCleanupLocked()
	return len(incoming)
}

func (s *metricStore) read(key metricKey, since float64, now time.Time) []metricSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	from := math.Max(since, float64(now.Add(-metricRetention).UnixMilli()))
	samples := s.series[key]
	start := sort.Search(len(samples), func(i int) bool { return samples[i].ObservedAt >= from })
	// Return an independent, non-nil slice: an empty history is {"samples":[]}.
	result := make([]metricSample, len(samples)-start)
	copy(result, samples[start:])
	return result
}

func (s *metricStore) pruneLocked(now time.Time) {
	oldest := float64(now.Add(-metricRetention).UnixMilli())
	for key, samples := range s.series {
		start := sort.Search(len(samples), func(i int) bool { return samples[i].ObservedAt >= oldest })
		if start == 0 {
			continue
		}
		s.total -= start
		if start == len(samples) {
			delete(s.series, key)
		} else {
			// Copy the live suffix so expired samples and nullable readings are released.
			s.series[key] = append([]metricSample(nil), samples[start:]...)
		}
	}
}

// Under global pressure, evict the device history whose newest sample is oldest.
func (s *metricStore) evictOldestLocked() {
	var oldestKey metricKey
	oldest := math.Inf(1)
	for key, samples := range s.series {
		if newest := samples[len(samples)-1].ObservedAt; newest < oldest {
			oldest = newest
			oldestKey = key
		}
	}
	s.total -= len(s.series[oldestKey])
	delete(s.series, oldestKey)
}

func (s *metricStore) scheduleCleanupLocked() {
	if len(s.series) > 0 && s.timer == nil {
		s.timer = time.AfterFunc(s.cleanupInterval, s.cleanup)
	}
}

func (s *metricStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timer = nil
	s.pruneLocked(time.Now())
	s.scheduleCleanupLocked()
}

func (c *connection) handleDeviceMetrics(payload []byte) {
	var message struct {
		BatchID json.RawMessage `json:"batchId"`
		Samples json.RawMessage `json:"samples"`
	}
	if json.Unmarshal(payload, &message) != nil {
		return
	}
	var samples []json.RawMessage
	if json.Unmarshal(message.Samples, &samples) != nil {
		samples = nil
	}
	accepted := c.metrics.store(metricKey{userID: c.hub.userID, deviceID: c.att.DeviceID}, samples, time.Now())
	_ = c.writeJSON(struct {
		Accepted int             `json:"accepted"`
		BatchID  json.RawMessage `json:"batchId,omitempty"`
		Type     string          `json:"type"`
	}{Accepted: accepted, BatchID: message.BatchID, Type: "device_metrics_ack"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	if body.DeviceID == "" {
		writeText(w, http.StatusBadRequest, "Missing deviceId")
		return
	}
	var since float64
	// Upstream defaults non-numeric or absent `since` to zero.
	_ = json.Unmarshal(body.Since, &since)
	samples := s.metrics.read(metricKey{userID: body.UserID, deviceID: body.DeviceID}, since, time.Now())
	writeJSON(w, http.StatusOK, map[string]any{"samples": samples})
}
