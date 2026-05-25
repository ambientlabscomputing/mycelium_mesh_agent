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
// NOTE: channel.bind.requested is handled by the unified link package (UNDF-140).
// This function registers only the route-register handler.
func RegisterHandlers(ec *discovery.EventConsumer, provider Provider, emitter *kernel.KernelEmitter) error {
	logger := logging.GetLogger(context.Background())
	logger.Info("registering channel event handlers")

	// Register dynamic channel route handler if the provider supports it.
	if hp, ok := provider.(*HyphaeProvider); ok {
		ec.RegisterHandler(types.EventChannelRouteRegister, func(ctx context.Context, event *types.UAEvent) error {
			return handleChannelRouteRegister(ctx, hp, event)
		})
	}

	logger.Info("channel event handlers registered")
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
