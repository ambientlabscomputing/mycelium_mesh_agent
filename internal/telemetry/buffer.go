package telemetry

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// bufferedEvent wraps an event with a timestamp
type bufferedEvent struct {
	event     interface{}
	timestamp time.Time
}

// Buffer collects and buffers telemetry and audit events
// Events are retained locally until flushed to UA
type Buffer struct {
	mu            sync.RWMutex
	events        []bufferedEvent // all events with timestamps
	maxSizeBytes  int
	currentSize   int
	flushTicker   *time.Ticker
	flushInterval time.Duration
	collectedAt   time.Time
}

// NewBuffer creates a new telemetry buffer
func NewBuffer(maxSizeBytes int, flushIntervalMs int) *Buffer {
	if maxSizeBytes <= 0 {
		maxSizeBytes = DefaultBufferSize
	}
	if maxSizeBytes > MaxBufferSize {
		maxSizeBytes = MaxBufferSize
	}
	if flushIntervalMs <= 0 {
		flushIntervalMs = DefaultFlushInterval
	}
	if flushIntervalMs < MinFlushInterval {
		flushIntervalMs = MinFlushInterval
	}
	return &Buffer{
		events:        make([]bufferedEvent, 0),
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

	// Check if buffer is at capacity - implement bounded buffer with drop-oldest strategy
	if b.currentSize+eventSize > b.maxSizeBytes && len(b.events) > 0 {
		// Remove oldest events until we have space
		removed := 0
		for b.currentSize+eventSize > b.maxSizeBytes && len(b.events) > removed {
			// Remove oldest event (FIFO)
			oldestData, _ := json.Marshal(b.events[removed].event)
			b.currentSize -= len(oldestData)
			removed++
		}

		// Shift array to remove dropped events
		if removed > 0 {
			b.events = b.events[removed:]
		}

		// If still can't fit, reject this single event if it's too large
		if eventSize > b.maxSizeBytes {
			return types.ErrTelemetryBufferFull
		}
	}

	// Wrap event with timestamp
	b.events = append(b.events, bufferedEvent{
		event:     event,
		timestamp: time.Now(),
	})
	b.currentSize += eventSize

	return nil
}

// GetEvents returns all buffered events and clears the buffer
func (b *Buffer) GetEvents() []interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Unwrap events from bufferedEvent wrappers
	result := make([]interface{}, 0, len(b.events))
	for _, be := range b.events {
		result = append(result, be.event)
	}

	b.events = make([]bufferedEvent, 0)
	b.currentSize = 0
	b.collectedAt = time.Now()

	return result
}

// GetEventsSince returns events recorded since given time
func (b *Buffer) GetEventsSince(since time.Time) []interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Filter events by timestamp and unwrap
	result := make([]interface{}, 0)
	retained := make([]bufferedEvent, 0)

	for _, be := range b.events {
		if be.timestamp.After(since) || be.timestamp.Equal(since) {
			result = append(result, be.event)
		} else {
			// Retain events that don't match the filter
			retained = append(retained, be)
		}
	}

	// Update buffer to only contain events not returned
	// Recalculate current size for retained events
	b.events = retained
	newSize := 0
	for _, be := range retained {
		data, _ := json.Marshal(be.event)
		newSize += len(data)
	}
	b.currentSize = newSize
	b.collectedAt = time.Now()

	return result
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

	b.events = make([]bufferedEvent, 0)
	b.currentSize = 0
}
