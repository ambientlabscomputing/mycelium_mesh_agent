package telemetry

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Buffer collects and buffers telemetry and audit events
// Events are retained locally until flushed to UA
type Buffer struct {
	mu            sync.RWMutex
	events        []interface{} // all events
	maxSizeBytes  int
	currentSize   int
	flushTicker   *time.Ticker
	flushInterval time.Duration
	collectedAt   time.Time
}

// NewBuffer creates a new telemetry buffer
func NewBuffer(maxSizeBytes int, flushIntervalMs int) *Buffer {
	return &Buffer{
		events:        make([]interface{}, 0),
		maxSizeBytes:  maxSizeBytes,
		flushInterval: time.Duration(flushIntervalMs) * time.Millisecond,
	}
}

// Start begins the flush timer
func (b *Buffer) Start(ctx context.Context) error {
	logger := logging.GetLogger(ctx)
	logger.Info("starting telemetry buffer")

	b.mu.Lock()
	b.collectedAt = time.Now()
	b.mu.Unlock()

	b.flushTicker = time.NewTicker(b.flushInterval)
	return nil
}

// Stop stops the flush timer
func (b *Buffer) Stop(ctx context.Context) error {
	logger := logging.GetLogger(ctx)
	logger.Info("stopping telemetry buffer")

	if b.flushTicker != nil {
		b.flushTicker.Stop()
	}
	return nil
}

// RecordBindingGranted records a binding grant event
func (b *Buffer) RecordBindingGranted(event *types.MeshBindingGrantedEvent) error {
	return b.recordEvent(event)
}

// RecordBindingDenied records a binding denied event
func (b *Buffer) RecordBindingDenied(event *types.MeshBindingDeniedEvent) error {
	return b.recordEvent(event)
}

// RecordBindingRevoked records a binding revoked event
func (b *Buffer) RecordBindingRevoked(event *types.MeshBindingRevokedEvent) error {
	return b.recordEvent(event)
}

// RecordPolicyDecision records a policy decision
func (b *Buffer) RecordPolicyDecision(event *types.MeshPolicyDecisionEvent) error {
	return b.recordEvent(event)
}

// RecordHealthChanged records a health state change
func (b *Buffer) RecordHealthChanged(event *types.MeshHealthChangedEvent) error {
	return b.recordEvent(event)
}

// recordEvent adds an event to the buffer
func (b *Buffer) recordEvent(event interface{}) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Calculate size of event in bytes
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	eventSize := len(data)

	// Check if adding this event would exceed buffer size
	if b.currentSize+eventSize > b.maxSizeBytes {
		// Buffer full, would need to flush or drop
		// For now, we'll allow overflow but log it
		// In production, might drop oldest events or return error
	}

	b.events = append(b.events, event)
	b.currentSize += eventSize

	return nil
}

// GetEvents returns all buffered events and clears the buffer
func (b *Buffer) GetEvents() []interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	events := b.events
	b.events = make([]interface{}, 0)
	b.currentSize = 0
	b.collectedAt = time.Now()

	return events
}

// GetEventsSince returns events recorded since given time
func (b *Buffer) GetEventsSince(since time.Time) []interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// For simplicity, return all events
	// In production, would need to timestamp events
	return b.events
}

// EventCount returns the number of buffered events
func (b *Buffer) EventCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events)
}

// BufferSize returns current buffer size in bytes
func (b *Buffer) BufferSize() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.currentSize
}

// Clear clears all buffered events
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.events = make([]interface{}, 0)
	b.currentSize = 0
}
