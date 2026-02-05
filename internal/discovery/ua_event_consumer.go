package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// EventConsumer listens to UA event stream and updates the registry
// gRPC streaming is used to consume events from UA
type EventConsumer struct {
	registry      *Registry
	config        UAEventConsumerConfig
	running       bool
	mu            sync.RWMutex
	stopChan      chan struct{}
	eventHandlers map[string]EventHandler
	processedSeq  map[string]int64 // track last seq per node to handle ordering
}

// UAEventConsumerConfig specifies EventConsumer configuration
type UAEventConsumerConfig struct {
	// StreamEndpoint is the gRPC endpoint for UA event stream
	StreamEndpoint string

	// ReconnectInterval is how often to reconnect if stream drops
	ReconnectInterval time.Duration

	// BufferSize is the channel buffer size for events
	BufferSize int
}

// EventHandler is called for each event
type EventHandler func(ctx context.Context, event *types.UAEvent) error

// NewEventConsumer creates a new UA event consumer
func NewEventConsumer(registry *Registry, config UAEventConsumerConfig) *EventConsumer {
	return &EventConsumer{
		registry:      registry,
		config:        config,
		stopChan:      make(chan struct{}),
		eventHandlers: make(map[string]EventHandler),
		processedSeq:  make(map[string]int64),
	}
}

// RegisterHandler registers a handler for specific event types
func (ec *EventConsumer) RegisterHandler(eventType string, handler EventHandler) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.eventHandlers[eventType] = handler
}

// Start begins consuming events from UA
func (ec *EventConsumer) Start(ctx context.Context) error {
	ec.mu.Lock()
	if ec.running {
		ec.mu.Unlock()
		return fmt.Errorf("event consumer already running")
	}
	ec.running = true
	ec.mu.Unlock()

	logger := logging.GetLogger(ctx)
	logger.Info("starting UA event consumer", "endpoint", ec.config.StreamEndpoint)

	// TODO: Implement gRPC streaming connection to UA
	// For now, this is a placeholder that demonstrates the structure

	// Start event processing loop
	go ec.processEventLoop(ctx)

	return nil
}

// Stop stops consuming events
func (ec *EventConsumer) Stop(ctx context.Context) error {
	ec.mu.Lock()
	if !ec.running {
		ec.mu.Unlock()
		return fmt.Errorf("event consumer not running")
	}
	ec.running = false
	ec.mu.Unlock()

	logger := logging.GetLogger(ctx)
	logger.Info("stopping UA event consumer")

	close(ec.stopChan)
	return nil
}

// IsRunning returns whether the consumer is running
func (ec *EventConsumer) IsRunning() bool {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	return ec.running
}

// processEventLoop processes events from the UA stream
func (ec *EventConsumer) processEventLoop(ctx context.Context) {
	logger := logging.GetLogger(ctx)

	// TODO: When gRPC stream is implemented, listen on it here
	// For now this is placeholder structure
	for {
		select {
		case <-ec.stopChan:
			logger.Debug("event consumer loop stopped")
			return
		case <-ctx.Done():
			logger.Debug("event consumer context cancelled")
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// HandleEvent processes a single event and updates registry
func (ec *EventConsumer) HandleEvent(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	// Check sequence to ensure ordering per node
	lastSeq := ec.processedSeq[event.NodeID]
	if event.Seq <= lastSeq && lastSeq > 0 {
		logger.Debug("skipping out-of-order event", "event_type", event.EventType, "node", event.NodeID, "seq", event.Seq, "last_seq", lastSeq)
		return nil
	}
	ec.processedSeq[event.NodeID] = event.Seq

	logger = logger.With(
		"event_type", event.EventType,
		"event_id", event.EventID,
		"node_id", event.NodeID,
	)

	// Call registered handler if exists
	if handler, exists := ec.eventHandlers[event.EventType]; exists {
		if err := handler(ctx, event); err != nil {
			logger.Error("handler error", "error", err)
			return err
		}
	}

	// Process event based on type
	switch event.EventType {
	case types.EventClusterSnapshot:
		return ec.handleClusterSnapshot(ctx, event)

	case types.EventMemberJoined:
		return ec.handleMemberJoined(ctx, event)

	case types.EventMemberUpdated:
		return ec.handleMemberUpdated(ctx, event)

	case types.EventMemberLeft:
		return ec.handleMemberLeft(ctx, event)

	case types.EventMemberHealth:
		return ec.handleMemberHealth(ctx, event)

	case types.EventServiceStarted:
		return ec.handleServiceStarted(ctx, event)

	case types.EventServiceUpdated:
		return ec.handleServiceUpdated(ctx, event)

	case types.EventServiceStopped:
		return ec.handleServiceStopped(ctx, event)

	case types.EventCapabilityCacheSnapshotUpdated:
		return ec.handleCapabilityCacheSnapshot(ctx, event)

	case types.EventCapabilityCacheDeltaUpdated:
		return ec.handleCapabilityCacheDelta(ctx, event)

	case types.EventIdentityTrustRootsUpdated:
		return ec.handleIdentityTrustRoots(ctx, event)

	case types.EventMeshPolicyUpdated:
		return ec.handleMeshPolicyUpdated(ctx, event)

	default:
		logger.Warn("unknown event type", "event_type", event.EventType)
		return nil
	}
}

// ===== Event Handlers =====

func (ec *EventConsumer) handleClusterSnapshot(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.ClusterSnapshotPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal cluster snapshot payload", "error", err)
		return err
	}

	// Clear existing members and rebuild from snapshot (snapshot semantics)
	ec.registry.Clear()
	logger.Info("cluster snapshot received", "members_count", len(payload.Members))

	// Clear existing members and rebuild (snapshot semantics)
	// In a real implementation, you might want to merge instead
	for _, member := range payload.Members {
		ec.registry.AddOrUpdateMember(&member)
	}

	return nil
}

func (ec *EventConsumer) handleMemberJoined(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.MemberJoinedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal member joined payload", "error", err)
		return err
	}

	member := &types.MemberInfo{
		NodeID:       payload.NodeID,
		NodeIdentity: payload.NodeIdentity,
		Endpoints:    payload.Endpoints,
		Tags:         payload.Tags,
		Status:       "healthy",
	}

	ec.registry.AddOrUpdateMember(member)
	logger.Info("member joined", "node_id", payload.NodeID)
	return nil
}

func (ec *EventConsumer) handleMemberUpdated(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.MemberUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal member updated payload", "error", err)
		return err
	}

	member := ec.registry.GetMember(payload.NodeID)
	if member == nil {
		logger.Warn("member not found for update", "node_id", payload.NodeID)
		return nil
	}

	if len(payload.Endpoints) > 0 {
		member.Endpoints = payload.Endpoints
	}
	if payload.Tags != nil {
		member.Tags = payload.Tags
	}
	if payload.Status != "" {
		member.Status = payload.Status
	}

	ec.registry.AddOrUpdateMember(member)
	logger.Info("member updated", "node_id", payload.NodeID)
	return nil
}

func (ec *EventConsumer) handleMemberLeft(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.MemberLeftPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal member left payload", "error", err)
		return err
	}

	ec.registry.RemoveMember(payload.NodeID)
	logger.Info("member left", "node_id", payload.NodeID, "reason", payload.Reason)
	return nil
}

func (ec *EventConsumer) handleMemberHealth(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.MemberHealthPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal member health payload", "error", err)
		return err
	}

	member := ec.registry.GetMember(payload.NodeID)
	if member == nil {
		logger.Warn("member not found for health update", "node_id", payload.NodeID)
		return nil
	}

	member.Status = payload.Status
	ec.registry.AddOrUpdateMember(member)
	logger.Debug("member health updated", "node_id", payload.NodeID, "status", payload.Status)
	return nil
}

func (ec *EventConsumer) handleServiceStarted(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.ServiceStartedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal service started payload", "error", err)
		return err
	}

	service := &types.ServiceEntry{
		ServiceID:            payload.ServiceID,
		ServiceIdentity:      payload.ServiceIdentity,
		NodeID:               payload.NodeID,
		Endpoints:            payload.Endpoints,
		CapabilitiesProvided: payload.CapabilitiesProvided,
		Labels:               payload.Labels,
		Status:               "running",
		LastHeartbeat:        time.Now(),
	}

	ec.registry.AddOrUpdateService(service)

	// Add providers for each capability
	for _, capRef := range payload.CapabilitiesProvided {
		for _, endpoint := range payload.Endpoints {
			provider := &types.Provider{
				ServiceID:       payload.ServiceID,
				ServiceIdentity: payload.ServiceIdentity,
				NodeID:          payload.NodeID,
				CapabilityID:    capRef.CapabilityID,
				TrustTier:       types.TrustTierOfficial, // TODO: Get from metadata
				Endpoint:        endpoint,
				Labels:          payload.Labels,
				Available:       true,
			}
			ec.registry.AddOrUpdateProvider(capRef.CapabilityID, provider)
		}
	}

	logger.Info("service started", "service_id", payload.ServiceID, "capabilities", len(payload.CapabilitiesProvided))
	return nil
}

func (ec *EventConsumer) handleServiceUpdated(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.ServiceUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal service updated payload", "error", err)
		return err
	}

	service := ec.registry.GetService(payload.ServiceID)
	if service == nil {
		logger.Warn("service not found for update", "service_id", payload.ServiceID)
		return nil
	}

	if len(payload.Endpoints) > 0 {
		service.Endpoints = payload.Endpoints
	}
	if payload.Labels != nil {
		service.Labels = payload.Labels
	}

	service.LastHeartbeat = time.Now()
	ec.registry.AddOrUpdateService(service)
	logger.Info("service updated", "service_id", payload.ServiceID)
	return nil
}

func (ec *EventConsumer) handleServiceStopped(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.ServiceStoppedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal service stopped payload", "error", err)
		return err
	}

	ec.registry.RemoveService(payload.ServiceID)
	logger.Info("service stopped", "service_id", payload.ServiceID, "reason", payload.Reason)
	return nil
}

func (ec *EventConsumer) handleCapabilityCacheSnapshot(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.CapabilityCacheSnapshotUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal capability cache snapshot payload", "error", err)
		return err
	}

	logger.Info("capability cache snapshot updated", "version", payload.CacheVersion, "digest", payload.SchemaIndexDigest)
	// TODO: Fetch actual capability definitions from UA using the ref
	return nil
}

func (ec *EventConsumer) handleCapabilityCacheDelta(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.CapabilityCacheDeltaUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal capability cache delta payload", "error", err)
		return err
	}

	logger.Info("capability cache delta updated", "version", payload.CacheVersion)
	// TODO: Fetch delta from UA and apply to capabilities
	return nil
}

func (ec *EventConsumer) handleIdentityTrustRoots(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.IdentityTrustRootsUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal identity trust roots payload", "error", err)
		return err
	}

	logger.Info("identity trust roots updated", "rotation_id", payload.RotationID, "valid_to", payload.ValidTo)
	// TODO: Update TLS trust roots for verification
	return nil
}

func (ec *EventConsumer) handleMeshPolicyUpdated(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.MeshPolicyUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal mesh policy updated payload", "error", err)
		return err
	}

	logger.Info("mesh policy updated", "version", payload.PolicyVersion, "effective_at", payload.EffectiveAt)
	// TODO: Notify policy evaluator of policy update
	return nil
}

// Helper to unmarshal payloads from JSON
func unmarshalPayload(payload interface{}, target interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
