package channel

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/kernel"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// RegisterHandlers registers channel event handlers with the event consumer.
// emitter may be nil; completion events will still be logged but not emitted.
func RegisterHandlers(ec *discovery.EventConsumer, provider Provider, emitter *kernel.KernelEmitter) error {
	logger := logging.GetLogger(context.Background())
	logger.Info("registering channel event handlers")

	ec.RegisterHandler(types.EventChannelBindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleChannelBindRequested(ctx, provider, emitter, event)
	})

	logger.Info("channel event handlers registered")
	return nil
}

// handleChannelBindRequested handles channel.bind.requested events.
func handleChannelBindRequested(ctx context.Context, provider Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With(
		"event_type", event.EventType,
		"event_id", event.EventID,
	)

	var payload types.ChannelBindRequestedPayload
	if err := unmarshalPayload(event.Payload, &payload); err != nil {
		logger.Error("failed to unmarshal channel bind requested payload", "error", err)
		return err
	}

	logger = logger.With(
		"channel_id", payload.ChannelID,
		"role", payload.Role,
		"org_id", payload.OrgID,
	)
	logger.Info("handling channel bind request")

	req := &BindRequest{
		ChannelID:        payload.ChannelID,
		OrgID:            payload.OrgID,
		Role:             payload.Role,
		Grant:            payload.Grant,
		HyphaeTunnelAddr: payload.HyphaeTunnelAddr,
		SourceServerID:   payload.SourceServerID,
		DestServerID:     payload.DestServerID,
		Purpose:          payload.Purpose,
	}

	err := provider.BindChannel(ctx, req)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"channel_id": payload.ChannelID,
			"role":       payload.Role,
			"status":     "active",
			"error":      "",
		}
		if err != nil {
			completedPayload["status"] = "error"
			completedPayload["error"] = err.Error()
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventChannelBindCompleted, "channel", payload.ChannelID, completedPayload); emitErr != nil {
			logger.Error("failed to emit channel bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("channel bind failed", "error", err)
		return err
	}

	logger.Info("channel bind successful", "role", payload.Role)
	return nil
}

// unmarshalPayload decodes the raw event payload into dst.
func unmarshalPayload(payload interface{}, dst interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}
