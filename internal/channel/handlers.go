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

	// Register dynamic channel route handler if the provider supports it.
	if hp, ok := provider.(*HyphaeProvider); ok {
		ec.RegisterHandler(types.EventChannelRouteRegister, func(ctx context.Context, event *types.UAEvent) error {
			return handleChannelRouteRegister(ctx, hp, event)
		})
	}

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
			"status":     "success",
			"error":      "",
		}
		if err != nil {
			completedPayload["status"] = "failure"
			completedPayload["error"] = err.Error()
		}
		// Include the initiator's local relay address so server_api can expose it.
		if localAddr, ok := provider.LocalAddr(payload.ChannelID); ok {
			completedPayload["local_addr"] = localAddr
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

// channelRouteRegisterPayload is the JSON shape of a channel.route.register event.
type channelRouteRegisterPayload struct {
	Purpose string `json:"purpose"`
	Addr    string `json:"addr"`
}

// handleChannelRouteRegister handles channel.route.register events from the agent.
// The agent emits this event after starting a local service listener (e.g. the
// secret-replication TCP handler) so that MMA can dynamically route inbound
// Hyphae streams to the correct local address.
func handleChannelRouteRegister(ctx context.Context, provider *HyphaeProvider, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With(
		"event_type", event.EventType,
		"event_id", event.EventID,
	)

	var payload channelRouteRegisterPayload
	if err := unmarshalPayload(event.Payload, &payload); err != nil {
		logger.Error("failed to unmarshal channel route register payload", "error", err)
		return err
	}
	if payload.Purpose == "" || payload.Addr == "" {
		logger.Error("channel route register payload missing purpose or addr",
			"purpose", payload.Purpose, "addr", payload.Addr)
		return fmt.Errorf("channel.route.register: purpose and addr are required")
	}

	provider.RegisterRoute(payload.Purpose, payload.Addr)
	return nil
}
