package exposure

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/kernel"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// RegisterHandlers registers exposure event handlers with the event consumer.
// emitter may be nil; if so, completion events are logged but not emitted.
func RegisterHandlers(ec *discovery.EventConsumer, provider Provider, emitter *kernel.KernelEmitter) error {
	logger := logging.GetLogger(context.Background())
	logger.Info("registering exposure event handlers")

	// Register handler for exposure bind requests
	ec.RegisterHandler(types.EventExposureBindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleExposureBindRequested(ctx, provider, emitter, event)
	})

	// Register handler for exposure unbind requests
	ec.RegisterHandler(types.EventExposureUnbindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleExposureUnbindRequested(ctx, provider, emitter, event)
	})

	// Register handler for tunnel bind requests (on-demand public tunnels)
	ec.RegisterHandler(types.EventTunnelBindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleTunnelBindRequested(ctx, provider, emitter, event)
	})

	// Register handler for tunnel unbind requests
	ec.RegisterHandler(types.EventTunnelUnbindRequested, func(ctx context.Context, event *types.UAEvent) error {
		return handleTunnelUnbindRequested(ctx, provider, emitter, event)
	})

	logger.Info("exposure event handlers registered")
	return nil
}

// handleExposureBindRequested handles exposure bind requested events
// This is called when server_api publishes an exposure.bind.request Spine event
// which is forwarded to MMA as EventExposureBindRequested
func handleExposureBindRequested(ctx context.Context, provider Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With(
		"event_type", event.EventType,
		"event_id", event.EventID,
	)

	// Unmarshal the payload
	payload := &types.ExposureBindRequestedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal exposure bind requested payload", "error", err)
		return err
	}

	logger = logger.With(
		"exposure_id", payload.ExposureID,
		"hostname", payload.Hostname,
		"target_port", payload.TargetPort,
	)

	logger.Info("handling exposure bind request")

	// Create bind request
	bindReq := &BindRequest{
		ExposureID: payload.ExposureID,
		LeaseID:    payload.LeaseID,
		Hostname:   payload.Hostname,
		TargetPort: int(payload.TargetPort),
		LocalAddr:  payload.LocalAddr,
		TunnelAddr: payload.HyphaeTunnelAddr,
	}

	// Call provider to establish tunnel
	bindResult, err := provider.Bind(ctx, bindReq)

	// Emit completion event regardless of outcome
	if emitter != nil {
		completedPayload := map[string]interface{}{
			"exposure_id": payload.ExposureID,
			"lease_id":    payload.LeaseID,
			"status":      "bound",
			"public_url":  "",
			"error":       "",
		}
		if err != nil {
			completedPayload["status"] = "error"
			completedPayload["error"] = err.Error()
		} else {
			completedPayload["public_url"] = bindResult.PublicURL
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventExposureBindCompleted, "exposure", payload.ExposureID, completedPayload); emitErr != nil {
			logger.Error("failed to emit exposure bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to bind tunnel", "error", err)
		return err
	}

	logger.Info("exposure bind successful",
		"lease_id", bindResult.LeaseID,
		"public_url", bindResult.PublicURL,
		"status", bindResult.Status,
	)

	return nil
}

// handleExposureUnbindRequested handles exposure unbind requested events
// This is called when server_api publishes an exposure.unbind.request Spine event
// which is forwarded to MMA as EventExposureUnbindRequested
func handleExposureUnbindRequested(ctx context.Context, provider Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With(
		"event_type", event.EventType,
		"event_id", event.EventID,
	)

	// Unmarshal the payload
	payload := &types.ExposureUnbindRequestedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal exposure unbind requested payload", "error", err)
		return err
	}

	logger = logger.With(
		"exposure_id", payload.ExposureID,
	)

	logger.Info("handling exposure unbind request")

	// Call provider to tear down tunnel
	err := provider.Unbind(ctx, payload.ExposureID)

	// Emit completion event regardless of outcome
	if emitter != nil {
		completedPayload := map[string]interface{}{
			"exposure_id": payload.ExposureID,
			"error":       "",
		}
		if err != nil {
			completedPayload["error"] = err.Error()
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventExposureUnbindCompleted, "exposure", payload.ExposureID, completedPayload); emitErr != nil {
			logger.Error("failed to emit exposure unbind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to unbind tunnel", "error", err)
		return err
	}

	logger.Info("exposure unbind successful")
	return nil
}

// handleTunnelBindRequested handles tunnel bind requests (EventTunnelBindRequested).
// It reuses the same Provider interface as exposures; the tunnel_id acts as the exposure ID.
func handleTunnelBindRequested(ctx context.Context, provider Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With("event_type", event.EventType, "event_id", event.EventID)

	payload := &types.TunnelBindRequestedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal tunnel bind requested payload", "error", err)
		return err
	}

	logger = logger.With("tunnel_id", payload.TunnelID, "hostname", payload.Hostname)
	logger.Info("handling tunnel bind request")

	// Resolve local address from target
	localAddr := ""
	targetURL := ""
	if payload.TargetType == "url" {
		targetURL = payload.Target
	} else {
		localAddr = fmt.Sprintf("localhost:%s", payload.Target)
	}

	bindReq := &BindRequest{
		ExposureID: payload.TunnelID, // reuse exposureID field
		LeaseID:    payload.LeaseID,
		Hostname:   payload.Hostname,
		LocalAddr:  localAddr,
		TargetURL:  targetURL,
		TunnelAddr: payload.HyphaeTunnelAddr,
	}

	bindResult, err := provider.Bind(ctx, bindReq)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"tunnel_id":  payload.TunnelID,
			"lease_id":   payload.LeaseID,
			"status":     "bound",
			"public_url": "",
			"error":      "",
		}
		if err != nil {
			completedPayload["status"] = "error"
			completedPayload["error"] = err.Error()
		} else {
			completedPayload["public_url"] = bindResult.PublicURL
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventTunnelBindCompleted, "tunnel", payload.TunnelID, completedPayload); emitErr != nil {
			logger.Error("failed to emit tunnel bind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to bind tunnel", "error", err)
		return err
	}

	logger.Info("tunnel bind successful", "public_url", bindResult.PublicURL)
	return nil
}

// handleTunnelUnbindRequested handles tunnel unbind requests (EventTunnelUnbindRequested).
func handleTunnelUnbindRequested(ctx context.Context, provider Provider, emitter *kernel.KernelEmitter, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx).With("event_type", event.EventType, "event_id", event.EventID)

	payload := &types.TunnelUnbindRequestedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal tunnel unbind requested payload", "error", err)
		return err
	}

	logger = logger.With("tunnel_id", payload.TunnelID)
	logger.Info("handling tunnel unbind request")

	err := provider.Unbind(ctx, payload.TunnelID)

	if emitter != nil {
		completedPayload := map[string]interface{}{
			"tunnel_id": payload.TunnelID,
			"error":     "",
		}
		if err != nil {
			completedPayload["error"] = err.Error()
		}
		if emitErr := emitter.EmitEvent(ctx, types.EventTunnelUnbindCompleted, "tunnel", payload.TunnelID, completedPayload); emitErr != nil {
			logger.Error("failed to emit tunnel unbind completed event", "error", emitErr)
		}
	}

	if err != nil {
		logger.Error("failed to unbind tunnel", "error", err)
		return err
	}

	logger.Info("tunnel unbind successful")
	return nil
}

// unmarshalPayload converts a payload map to a typed struct.
// Uses json marshaling as a bridge from interface{} to typed struct.
func unmarshalPayload(payload interface{}, target interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return nil
}
