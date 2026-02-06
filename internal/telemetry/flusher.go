package telemetry

import (
	"context"
	"fmt"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Flusher handles delivery of telemetry to UA
// Events are delivered via the gRPC control channel to UA
type Flusher struct {
	buffer    *Buffer
	collector *Collector
}

// NewFlusher creates a new flusher
func NewFlusher(buffer *Buffer, collector *Collector) *Flusher {
	return &Flusher{
		buffer:    buffer,
		collector: collector,
	}
}

// FlushRequest processes a flush request from UA
// Returns telemetry data and updates internal state
func (f *Flusher) FlushRequest(ctx context.Context, req *types.TelemetryFlushRequest) (*types.TelemetryFlushResponse, error) {
	logger := logging.GetLogger(ctx)

	logger.Info("processing telemetry flush request", "types", req.Types)

	response := &types.TelemetryFlushResponse{
		Delivered: types.TelemetryCounters{},
		Events:    make([]interface{}, 0),
	}

	// Determine what to flush based on request types
	shouldFlushAudit := false
	shouldFlushMetrics := false

	for _, t := range req.Types {
		if t == "audit" {
			shouldFlushAudit = true
		} else if t == "metrics" {
			shouldFlushMetrics = true
		}
	}

	// Flush audit events from buffer
	if shouldFlushAudit {
		events := f.buffer.GetEvents()
		response.Events = append(response.Events, events...)

		auditCount := len(events)
		logger.Info("flushed audit events", "count", auditCount)
	}

	// Flush metrics if needed
	if shouldFlushMetrics {
		if f.collector.ShouldGenerateSummary() {
			summary := f.collector.GenerateSummary()
			response.Events = append(response.Events, summary)

			response.Delivered.BindingsGranted = summary.Counts.BindingsGranted
			response.Delivered.BindingsDenied = summary.Counts.BindingsDenied
			response.Delivered.BindingsRevoked = summary.Counts.BindingsRevoked
			response.Delivered.ConnectionsOpen = summary.Counts.ConnectionsOpen
			response.Delivered.BytesTransferred = summary.Counts.BytesTransferred

			logger.Info("flushed metrics", "bindings_granted", summary.Counts.BindingsGranted, "bindings_denied", summary.Counts.BindingsDenied)

			f.collector.ResetCounters()
		}
	}

	// Calculate remaining queued
	response.RemainingQueued = int64(f.buffer.EventCount())

	logger.Info("telemetry flush complete", "events_delivered", len(response.Events), "events_remaining", response.RemainingQueued)

	return response, nil
}

// SendTelemetrySummary sends a telemetry summary to UA
// This is called periodically even without an explicit flush request
func (f *Flusher) SendTelemetrySummary(ctx context.Context) (*types.MeshTelemetrySummary, error) {
	logger := logging.GetLogger(ctx)

	if !f.collector.ShouldGenerateSummary() {
		return nil, fmt.Errorf("summary interval not reached")
	}

	summary := f.collector.GenerateSummary()
	logger.Debug("generated telemetry summary", "interval", summary.Interval)
	if err := f.BufferEvent(ctx, summary); err != nil {
		logger.Error("failed to buffer telemetry summary", "error", err)
		return nil, err
	}
	return summary, nil
}

// BufferEvent adds an event to the buffer
// Called by other components to record events
func (f *Flusher) BufferEvent(ctx context.Context, event interface{}) error {
	return f.buffer.recordEvent(event)
}

// GetMetrics returns current metrics snapshot
func (f *Flusher) GetMetrics(ctx context.Context) map[string]interface{} {
	return f.collector.GetMetrics()
}

// GetHealthStatus returns the health status based on metrics
func (f *Flusher) GetHealthStatus(ctx context.Context) string {
	return f.collector.HealthStatus(ctx)
}
