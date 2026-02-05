package telemetry

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Collector gathers metrics and statistics
type Collector struct {
	mu sync.RWMutex

	// Counters
	bindingsGranted  int64
	bindingsDenied   int64
	bindingsRevoked  int64
	bindingsExpired  int64
	connectionsOpen  int64
	bytesTransferred int64

	// Error tracking
	denialReasons   map[types.ReasonCode]int64
	topCapabilities map[string]int64

	// Connection tracking
	activeConnections map[string]time.Time // keyed by binding_id

	// Interval for summaries
	summaryInterval time.Duration
	lastSummary     time.Time
}

// NewCollector creates a new metrics collector
func NewCollector(summaryIntervalMs int) *Collector {
	return &Collector{
		denialReasons:     make(map[types.ReasonCode]int64),
		topCapabilities:   make(map[string]int64),
		activeConnections: make(map[string]time.Time),
		summaryInterval:   time.Duration(summaryIntervalMs) * time.Millisecond,
		lastSummary:       time.Now(),
	}
}

// ===== Binding Metrics =====

// RecordBindingGranted increments granted counter
func (c *Collector) RecordBindingGranted(capabilityID string) {
	atomic.AddInt64(&c.bindingsGranted, 1)

	c.mu.Lock()
	c.topCapabilities[capabilityID]++
	c.mu.Unlock()
}

// RecordBindingDenied increments denied counter and tracks reason
func (c *Collector) RecordBindingDenied(capabilityID string, reason types.ReasonCode) {
	atomic.AddInt64(&c.bindingsDenied, 1)

	c.mu.Lock()
	c.denialReasons[reason]++
	c.mu.Unlock()
}

// RecordBindingRevoked increments revoked counter
func (c *Collector) RecordBindingRevoked() {
	atomic.AddInt64(&c.bindingsRevoked, 1)
}

// RecordBindingExpired increments expired counter
func (c *Collector) RecordBindingExpired() {
	atomic.AddInt64(&c.bindingsExpired, 1)
}

// ===== Connection Metrics =====

// RecordConnectionOpened tracks an open connection
func (c *Collector) RecordConnectionOpened(bindingID string) {
	atomic.AddInt64(&c.connectionsOpen, 1)

	c.mu.Lock()
	c.activeConnections[bindingID] = time.Now()
	c.mu.Unlock()
}

// RecordConnectionClosed tracks a closed connection
func (c *Collector) RecordConnectionClosed(bindingID string) {
	atomic.AddInt64(&c.connectionsOpen, -1)

	c.mu.Lock()
	delete(c.activeConnections, bindingID)
	c.mu.Unlock()
}

// RecordBytesTransferred tracks data transfer
func (c *Collector) RecordBytesTransferred(bytes int64) {
	atomic.AddInt64(&c.bytesTransferred, bytes)
}

// ===== Summary Generation =====

// ShouldGenerateSummary checks if enough time has passed for a summary
func (c *Collector) ShouldGenerateSummary() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return time.Since(c.lastSummary) >= c.summaryInterval
}

// GenerateSummary creates a telemetry summary
func (c *Collector) GenerateSummary() *types.MeshTelemetrySummary {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect top denies
	topDenies := make([]types.DenyEntry, 0)
	for reason, count := range c.denialReasons {
		topDenies = append(topDenies, types.DenyEntry{
			ReasonCode: reason,
			Count:      count,
		})
	}

	// Collect top capabilities
	topCaps := make([]types.CapabilityEntry, 0)
	for capID, count := range c.topCapabilities {
		topCaps = append(topCaps, types.CapabilityEntry{
			CapabilityID: capID,
			RequestCount: count,
		})
	}

	summary := &types.MeshTelemetrySummary{
		Interval: c.summaryInterval.String(),
		Counts: types.TelemetryCounters{
			BindingsGranted:  atomic.LoadInt64(&c.bindingsGranted),
			BindingsDenied:   atomic.LoadInt64(&c.bindingsDenied),
			BindingsRevoked:  atomic.LoadInt64(&c.bindingsRevoked),
			ConnectionsOpen:  atomic.LoadInt64(&c.connectionsOpen),
			BytesTransferred: atomic.LoadInt64(&c.bytesTransferred),
		},
		ErrorRates:      make(map[string]float64),
		TopDenies:       topDenies,
		TopCapabilities: topCaps,
		BufferStats: types.BufferStatistics{
			TelemetryBytes:    int64(len(topDenies) + len(topCaps)),
			AuditEventsQueued: int64(len(c.activeConnections)),
		},
		CollectedAt: time.Now(),
	}

	c.lastSummary = time.Now()

	// Calculate error rates
	if summary.Counts.BindingsGranted+summary.Counts.BindingsDenied > 0 {
		total := float64(summary.Counts.BindingsGranted + summary.Counts.BindingsDenied)
		summary.ErrorRates["deny_rate"] = float64(summary.Counts.BindingsDenied) / total
	}

	return summary
}

// ResetCounters resets all counters (called after flushing)
func (c *Collector) ResetCounters() {
	atomic.StoreInt64(&c.bindingsGranted, 0)
	atomic.StoreInt64(&c.bindingsDenied, 0)
	atomic.StoreInt64(&c.bindingsRevoked, 0)
	atomic.StoreInt64(&c.bindingsExpired, 0)
	atomic.StoreInt64(&c.bytesTransferred, 0)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.denialReasons = make(map[types.ReasonCode]int64)
	c.topCapabilities = make(map[string]int64)
}

// ===== Observability =====

// GetMetrics returns current metrics
func (c *Collector) GetMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"bindings_granted":   atomic.LoadInt64(&c.bindingsGranted),
		"bindings_denied":    atomic.LoadInt64(&c.bindingsDenied),
		"bindings_revoked":   atomic.LoadInt64(&c.bindingsRevoked),
		"bindings_expired":   atomic.LoadInt64(&c.bindingsExpired),
		"connections_open":   atomic.LoadInt64(&c.connectionsOpen),
		"bytes_transferred":  atomic.LoadInt64(&c.bytesTransferred),
		"active_connections": len(c.activeConnections),
	}
}

// HealthStatus provides health indication based on metrics
func (c *Collector) HealthStatus(ctx context.Context) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	granted := atomic.LoadInt64(&c.bindingsGranted)
	denied := atomic.LoadInt64(&c.bindingsDenied)
	total := granted + denied

	if total == 0 {
		return "ready"
	}

	denyRate := float64(denied) / float64(total)
	if denyRate > 0.5 {
		logger := logging.GetLogger(ctx)
		logger.Warn("high denial rate", "deny_rate", denyRate)
		return "degraded"
	}

	return "ready"
}
