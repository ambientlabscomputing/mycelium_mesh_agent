package link

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/channel"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/exposure"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/kernel"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// RegisterHandlers registers the single unified link bind/unbind handler with the
// event consumer. One handler dispatches to per-kind worker logic based on Link.Kind;
// only one handler pair is wired to Spine (link.bind.requested / link.unbind.requested).
func RegisterHandlers(ec *discovery.EventConsumer, expProvider exposure.Provider, chanProvider channel.Provider, emitter *kernel.KernelEmitter) error {
	ec.RegisterHandler(types.EventLinkBindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleLinkBindRequested(ctx, expProvider, chanProvider, emitter, event)
	})

	ec.RegisterHandler(types.EventLinkUnbindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleLinkUnbindRequested(ctx, expProvider, chanProvider, emitter, event)
	})

	return nil
}

// handleLinkBindRequested is the single MMA bind handler. It unmarshals the unified
// LinkBindRequestedPayload and dispatches to per-kind worker logic.
func handleLinkBindRequested(ctx context.Context, expProvider exposure.Provider, chanProvider channel.Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With("event_type", event.EventType, "event_id", event.EventID)

	var payload types.LinkBindRequestedPayload
	if err := unmarshalPayload(event.Payload, &payload); err != nil {
		logger.Error("failed to unmarshal link bind requested payload", "error", err)
		return err
	}

	logger = logger.With("link_id", payload.LinkID, "kind", payload.Kind, "hostname", payload.Hostname)
	logger.Info("handling link bind request")

	switch payload.Kind {
	case "exposure":
		return bindExposure(ctx, expProvider, emitter, payload)
	case "tunnel":
		return bindTunnel(ctx, expProvider, emitter, payload)
	case "channel":
		return bindChannel(ctx, chanProvider, emitter, payload)
	default:
		return fmt.Errorf("handleLinkBindRequested: unknown link kind %q", payload.Kind)
	}
}

// handleLinkUnbindRequested is the single MMA unbind handler.
func handleLinkUnbindRequested(ctx context.Context, expProvider exposure.Provider, chanProvider channel.Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With("event_type", event.EventType, "event_id", event.EventID)

	var payload types.LinkUnbindRequestedPayload
	if err := unmarshalPayload(event.Payload, &payload); err != nil {
		logger.Error("failed to unmarshal link unbind requested payload", "error", err)
		return err
	}

	logger = logger.With("link_id", payload.LinkID, "kind", payload.Kind)
	logger.Info("handling link unbind request")

	switch payload.Kind {
	case "exposure", "tunnel":
		return unbindExposureOrTunnel(ctx, expProvider, emitter, payload)
	case "channel":
		return unbindChannel(ctx, chanProvider, emitter, payload)
	default:
		return fmt.Errorf("handleLinkUnbindRequested: unknown link kind %q", payload.Kind)
	}
}

// ===== Per-kind worker functions =====

func bindExposure(ctx context.Context, provider exposure.Provider, emitter *kernel.KernelEmitter, payload types.LinkBindRequestedPayload) error {
	logger := logging.GetLogger(ctx).With("link_id", payload.LinkID, "kind", "exposure")

	bindReq := &exposure.BindRequest{
		ExposureID: payload.LinkID,
		LeaseID:    payload.Spec.LeaseID,
		Hostname:   payload.Hostname,
		TargetPort: payload.Spec.TargetPort,
		LocalAddr:  payload.Spec.LocalAddr,
		TunnelAddr: payload.HyphaeTunnelAddr,
	}

	bindResult, err := provider.Bind(ctx, bindReq)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"link_id":    payload.LinkID,
			"kind":       "exposure",
			"status":     "success",
			"public_url": "",
			"error":      "",
		}
		if err != nil {
			completedPayload["status"] = "failure"
			completedPayload["error"] = err.Error()
		} else if bindResult != nil {
			completedPayload["public_url"] = bindResult.PublicURL
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventLinkBindCompleted, "link", payload.LinkID, completedPayload); emitErr != nil {
			logger.Error("failed to emit link bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to bind exposure", "error", err)
		return err
	}
	logger.Info("exposure bind successful", "public_url", bindResult.PublicURL)
	return nil
}

func bindTunnel(ctx context.Context, provider exposure.Provider, emitter *kernel.KernelEmitter, payload types.LinkBindRequestedPayload) error {
	logger := logging.GetLogger(ctx).With("link_id", payload.LinkID, "kind", "tunnel")

	// Resolve local address from target.
	localAddr := ""
	targetURL := ""
	if payload.Spec.TargetType == "url" {
		targetURL = payload.Spec.Target
	} else {
		localAddr = fmt.Sprintf("localhost:%s", payload.Spec.Target)
	}

	bindReq := &exposure.BindRequest{
		ExposureID: payload.LinkID, // reuse exposureID field; provider is kind-agnostic
		LeaseID:    payload.Spec.LeaseID,
		Hostname:   payload.Hostname,
		LocalAddr:  localAddr,
		TargetURL:  targetURL,
		TunnelAddr: payload.HyphaeTunnelAddr,
	}

	bindResult, err := provider.Bind(ctx, bindReq)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"link_id":    payload.LinkID,
			"kind":       "tunnel",
			"status":     "success",
			"public_url": "",
			"error":      "",
		}
		if err != nil {
			completedPayload["status"] = "failure"
			completedPayload["error"] = err.Error()
		} else if bindResult != nil {
			completedPayload["public_url"] = bindResult.PublicURL
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventLinkBindCompleted, "link", payload.LinkID, completedPayload); emitErr != nil {
			logger.Error("failed to emit link bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to bind tunnel", "error", err)
		return err
	}
	logger.Info("tunnel bind successful", "public_url", bindResult.PublicURL)
	return nil
}

func bindChannel(ctx context.Context, provider channel.Provider, emitter *kernel.KernelEmitter, payload types.LinkBindRequestedPayload) error {
	logger := logging.GetLogger(ctx).With("link_id", payload.LinkID, "kind", "channel", "role", payload.Spec.Role)

	bindReq := &channel.BindRequest{
		ChannelID:        payload.LinkID,
		OrgID:            payload.OrgID,
		Role:             payload.Spec.Role,
		Grant:            payload.Spec.Grant,
		HyphaeTunnelAddr: payload.HyphaeTunnelAddr,
		SourceServerID:   payload.Spec.SourceServerID,
		DestServerID:     payload.Spec.DestServerID,
		Purpose:          payload.Spec.Purpose,
	}

	err := provider.BindChannel(ctx, bindReq)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"link_id": payload.LinkID,
			"kind":    "channel",
			"role":    payload.Spec.Role,
			"status":  "success",
			"error":   "",
		}
		if err != nil {
			completedPayload["status"] = "failure"
			completedPayload["error"] = err.Error()
		}
		// Include the initiator's local relay address so server_api can store it.
		if localAddr, ok := provider.LocalAddr(payload.LinkID); ok {
			completedPayload["local_addr"] = localAddr
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventLinkBindCompleted, "link", payload.LinkID, completedPayload); emitErr != nil {
			logger.Error("failed to emit link bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("channel bind failed", "error", err)
		return err
	}
	logger.Info("channel bind successful", "role", payload.Spec.Role)
	return nil
}

func unbindExposureOrTunnel(ctx context.Context, provider exposure.Provider, emitter *kernel.KernelEmitter, payload types.LinkUnbindRequestedPayload) error {
	logger := logging.GetLogger(ctx).With("link_id", payload.LinkID, "kind", payload.Kind)

	err := provider.Unbind(ctx, payload.LinkID)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"link_id": payload.LinkID,
			"kind":    payload.Kind,
			"error":   "",
		}
		if err != nil {
			completedPayload["error"] = err.Error()
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventLinkUnbindCompleted, "link", payload.LinkID, completedPayload); emitErr != nil {
			logger.Error("failed to emit link unbind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to unbind", "error", err)
		return err
	}
	logger.Info("unbind successful")
	return nil
}

func unbindChannel(ctx context.Context, provider channel.Provider, emitter *kernel.KernelEmitter, payload types.LinkUnbindRequestedPayload) error {
	logger := logging.GetLogger(ctx).With("link_id", payload.LinkID, "kind", "channel")

	err := provider.UnbindChannel(ctx, payload.LinkID)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"link_id": payload.LinkID,
			"kind":    "channel",
			"error":   "",
		}
		if err != nil {
			completedPayload["error"] = err.Error()
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventLinkUnbindCompleted, "link", payload.LinkID, completedPayload); emitErr != nil {
			logger.Error("failed to emit link unbind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("channel unbind failed", "error", err)
		return err
	}
	logger.Info("channel unbind successful")
	return nil
}

// unmarshalPayload converts an interface{} payload to a typed struct via JSON round-trip.
func unmarshalPayload(payload interface{}, target interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}
