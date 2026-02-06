package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/policy_evaluator"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/transport"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
	pb "github.com/ambientlabscomputing/mycelium_mesh_agent/proto/ua_mma/v1"
)

// EventConsumer listens to UA event stream and updates the registry
// gRPC streaming is used to consume events from UA
type EventConsumer struct {
	registry      *Registry
	config        config.ControlChannelConfig
	policy        policy_evaluator.PolicyEvaluator
	running       bool
	mu            sync.RWMutex
	stopChan      chan struct{}
	eventCh       chan *types.UAEvent
	eventHandlers map[string]EventHandler
	nodeSequences map[string]uint64 // track last seq per node for resumption
	trustRoots    *types.IdentityTrustRootsUpdatedPayload
	tlsCreds      *transport.TLSCredentials
	grpcConn      *grpc.ClientConn
}

// UAEventConsumerConfig specifies EventConsumer configuration (deprecated, use ControlChannelConfig)
type UAEventConsumerConfig struct {
	// StreamEndpoint is the gRPC endpoint for UA event stream
	StreamEndpoint string

	// ReconnectInterval is how often to reconnect if stream drops
	ReconnectInterval time.Duration

	// BufferSize is the channel buffer size for events
	BufferSize int
}

const (
	// MaxReconnectBackoff is the maximum backoff duration between reconnect attempts
	MaxReconnectBackoff = 5 * time.Minute

	// InitialReconnectBackoff is the initial backoff duration
	InitialReconnectBackoff = 1 * time.Second

	// BackoffMultiplier is the exponential backoff multiplier
	BackoffMultiplier = 2.0
)

// EventHandler is called for each event
type EventHandler func(ctx context.Context, event *types.UAEvent) error

// NewEventConsumer creates a new UA event consumer
func NewEventConsumer(registry *Registry, cfg config.ControlChannelConfig) *EventConsumer {
	bufferSize := 1000
	if cfg.Address == "" {
		cfg.Address = "/tmp/ua_mma.sock"
	}
	if cfg.Transport == "" {
		cfg.Transport = "grpc_uds"
	}

	return &EventConsumer{
		registry:      registry,
		config:        cfg,
		stopChan:      make(chan struct{}),
		eventCh:       make(chan *types.UAEvent, bufferSize),
		eventHandlers: make(map[string]EventHandler),
		nodeSequences: make(map[string]uint64),
	}
}

// SetPolicyEvaluator sets the policy evaluator for policy update events.
func (ec *EventConsumer) SetPolicyEvaluator(policy policy_evaluator.PolicyEvaluator) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.policy = policy
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
	defer ec.mu.Unlock()

	if ec.running {
		return fmt.Errorf("event consumer already running")
	}

	logger := logging.GetLogger(ctx)
	logger.Info("starting UA event consumer", "transport", ec.config.Transport, "address", ec.config.Address)

	// Reset stop channel for fresh start
	ec.stopChan = make(chan struct{})

	ec.running = true

	// Initialize TLS credentials if enabled
	if ec.config.TLSEnabled {
		tlsCreds, err := transport.NewTLSCredentials(ec.config)
		if err != nil {
			ec.running = false
			return fmt.Errorf("failed to create TLS credentials: %w", err)
		}
		ec.tlsCreds = tlsCreds
	}

	// Start event stream based on transport type
	if strings.HasPrefix(ec.config.Address, "file://") {
		// File-based testing mode
		path := strings.TrimPrefix(ec.config.Address, "file://")
		go ec.streamFromFile(ctx, path)
	} else {
		// Real gRPC streaming
		go ec.streamFromGRPC(ctx)
	}

	// Start event processing loop
	go ec.processEventLoop(ctx)

	logger.Info("event consumer started")
	return nil
}

// Stop stops consuming events
func (ec *EventConsumer) Stop(ctx context.Context) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if !ec.running {
		return fmt.Errorf("event consumer not running")
	}

	ec.running = false

	logger := logging.GetLogger(ctx)
	logger.Info("stopping UA event consumer")

	close(ec.stopChan)

	// Close gRPC connection if open
	if ec.grpcConn != nil {
		if err := ec.grpcConn.Close(); err != nil {
			logger.Error("failed to close gRPC connection", "error", err)
		}
		ec.grpcConn = nil
	}

	return nil
}

// streamFromGRPC establishes a gRPC stream to the UA and consumes events.
// It implements exponential backoff reconnect logic.
func (ec *EventConsumer) streamFromGRPC(ctx context.Context) {
	logger := logging.GetLogger(ctx)
	backoff := InitialReconnectBackoff
	attempt := 0

	for {
		select {
		case <-ec.stopChan:
			logger.Info("gRPC stream stopped")
			close(ec.eventCh)
			return
		case <-ctx.Done():
			logger.Info("gRPC stream context cancelled")
			close(ec.eventCh)
			return
		default:
		}

		attempt++
		logger.Info("attempting to connect to UA event stream", "attempt", attempt, "address", ec.config.Address)

		// Establish gRPC connection
		dialOpts := []grpc.DialOption{}

		if ec.config.Transport == "grpc_uds" {
			// Unix domain socket
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return net.Dial("unix", addr)
			}))
		} else if ec.config.TLSEnabled && ec.tlsCreds != nil {
			// TCP with TLS
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(ec.tlsCreds.GetTransportCredentials()))
		} else {
			// TCP without TLS (insecure)
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		conn, err := grpc.DialContext(ctx, ec.config.Address, dialOpts...)
		if err != nil {
			logger.Error("failed to dial UA", "error", err, "backoff", backoff)
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(backoff)*BackoffMultiplier, float64(MaxReconnectBackoff)))
			continue
		}

		ec.mu.Lock()
		ec.grpcConn = conn
		ec.mu.Unlock()

		client := pb.NewUAEventStreamServiceClient(conn)

		// Build StreamEventsRequest with last_seen_seq for resumption
		req := &pb.StreamEventsRequest{
			LastSeenSeq:     ec.getLastSeenSeq(),
			EventTypeFilter: []string{}, // Subscribe to all event types
			BufferSizeHint:  1000,
		}

		stream, err := client.StreamEvents(ctx, req)
		if err != nil {
			logger.Error("failed to start stream", "error", err, "backoff", backoff)
			conn.Close()
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(backoff)*BackoffMultiplier, float64(MaxReconnectBackoff)))
			continue
		}

		logger.Info("gRPC event stream connected")
		backoff = InitialReconnectBackoff // Reset backoff on successful connection

		// Consume stream
		streamErr := ec.consumeStream(ctx, stream)
		conn.Close()

		if streamErr != nil {
			if status.Code(streamErr) == codes.Canceled {
				logger.Info("stream cancelled")
				close(ec.eventCh)
				return
			}
			logger.Error("stream error", "error", streamErr, "backoff", backoff)
		}

		// Exponential backoff before reconnect
		time.Sleep(backoff)
		backoff = time.Duration(math.Min(float64(backoff)*BackoffMultiplier, float64(MaxReconnectBackoff)))
	}
}

// consumeStream reads events from the gRPC stream and pushes them to eventCh.
func (ec *EventConsumer) consumeStream(ctx context.Context, stream pb.UAEventStreamService_StreamEventsClient) error {
	logger := logging.GetLogger(ctx)

	for {
		select {
		case <-ec.stopChan:
			return status.Error(codes.Canceled, "stopped")
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pbEvent, err := stream.Recv()
		if err != nil {
			return err
		}

		logger.Info("received event from UA", "event_type", pbEvent.EventType, "event_id", pbEvent.EventId, "seq", pbEvent.Seq)

		// Convert pb.UAEvent to types.UAEvent
		event, err := ec.convertPBEvent(pbEvent)
		if err != nil {
			logger.Error("failed to convert event", "error", err, "event_id", pbEvent.EventId)
			continue
		}

		// Push to event channel (sequence will be updated after processing in HandleEvent)
		select {
		case ec.eventCh <- event:
		case <-ec.stopChan:
			return status.Error(codes.Canceled, "stopped")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// convertPBEvent converts a protobuf UAEvent to types.UAEvent.
func (ec *EventConsumer) convertPBEvent(pbEvent *pb.UAEvent) (*types.UAEvent, error) {
	// Convert protobuf.Struct to map[string]interface{}
	payload := pbEvent.Payload.AsMap()

	event := &types.UAEvent{
		EventID:   pbEvent.EventId,
		EventType: pbEvent.EventType,
		EmittedAt: pbEvent.EmittedAt.AsTime(),
		ClusterID: pbEvent.ClusterId,
		NodeID:    pbEvent.NodeId,
		Seq:       pbEvent.Seq,
		Payload:   payload,
		Signature: pbEvent.Signature,
	}

	if pbEvent.EntityRef != nil {
		event.EntityRef = &types.EntityRef{
			Kind: pbEvent.EntityRef.Kind,
			ID:   pbEvent.EntityRef.Id,
		}
	}

	return event, nil
}

// getLastSeenSeq returns a copy of the node sequence map for resumption.
func (ec *EventConsumer) getLastSeenSeq() map[string]uint64 {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	m := make(map[string]uint64, len(ec.nodeSequences))
	for k, v := range ec.nodeSequences {
		m[k] = v
	}
	return m
}

// streamFromFile reads UA events from a JSON file and pushes them into the event channel.
func (ec *EventConsumer) streamFromFile(ctx context.Context, path string) {
	logger := logging.GetLogger(ctx)
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error("failed to read event stream file", "error", err, "path", path)
		close(ec.eventCh)
		return
	}

	var events []*types.UAEvent
	if err := json.Unmarshal(data, &events); err != nil {
		logger.Error("failed to parse event stream file", "error", err, "path", path)
		close(ec.eventCh)
		return
	}

	for _, event := range events {
		select {
		case <-ec.stopChan:
			logger.Info("event stream stopped")
			close(ec.eventCh)
			return
		case <-ctx.Done():
			logger.Info("event stream context cancelled")
			close(ec.eventCh)
			return
		case ec.eventCh <- event:
		}
	}

	close(ec.eventCh)
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

	for {
		select {
		case <-ec.stopChan:
			logger.Debug("event consumer loop stopped")
			return
		case <-ctx.Done():
			logger.Debug("event consumer context cancelled")
			return
		case event, ok := <-ec.eventCh:
			if !ok {
				logger.Debug("event stream closed")
				return
			}
			if err := ec.HandleEvent(ctx, event); err != nil {
				logger.Error("failed to handle event", "error", err, "event_type", event.EventType)
			}
		}
	}
}

// HandleEvent processes a single event and updates registry
func (ec *EventConsumer) HandleEvent(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	// Check sequence to ensure ordering per node
	ec.mu.RLock()
	lastSeq := ec.nodeSequences[event.NodeID]
	ec.mu.RUnlock()

	if event.Seq <= lastSeq && lastSeq > 0 {
		logger.Debug("skipping out-of-order event", "event_type", event.EventType, "node", event.NodeID, "seq", event.Seq, "last_seq", lastSeq)
		return nil
	}

	// Update sequence tracking for this node
	ec.mu.Lock()
	ec.nodeSequences[event.NodeID] = event.Seq
	ec.mu.Unlock()

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

	case types.EventIdentityServiceIssued:
		return ec.handleIdentityServiceIssued(ctx, event)

	case types.EventIdentityServiceRevoked:
		return ec.handleIdentityServiceRevoked(ctx, event)

	case types.EventMeshPolicyUpdated:
		return ec.handleMeshPolicyUpdated(ctx, event)

	case types.EventConsentStateUpdated:
		return ec.handleConsentStateUpdated(ctx, event)

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
			trustTier := types.TrustTierLocal
			if payload.Labels != nil {
				if tier, ok := payload.Labels["trust_tier"]; ok {
					switch types.TrustTier(tier) {
					case types.TrustTierLocal, types.TrustTierExperimental, types.TrustTierCommunity, types.TrustTierCertified, types.TrustTierOfficial:
						trustTier = types.TrustTier(tier)
					}
				}
			}

			provider := &types.Provider{
				ServiceID:       payload.ServiceID,
				ServiceIdentity: payload.ServiceIdentity,
				NodeID:          payload.NodeID,
				CapabilityID:    capRef.CapabilityID,
				TrustTier:       trustTier,
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
	if payload.SignedSnapshotRef != "" {
		cache := &types.CapabilityCache{}
		if err := loadJSONFromRef(payload.SignedSnapshotRef, cache); err != nil {
			logger.Error("failed to load capability cache from ref", "error", err, "ref", payload.SignedSnapshotRef)
			return err
		}
		cache.Version = payload.CacheVersion
		cache.SchemaIndexDigest = payload.SchemaIndexDigest
		cache.VerifiedAt = payload.VerifiedAt
		ec.registry.ReplaceCapabilityCache(cache)
		logger.Info("capability cache loaded", "capabilities", len(cache.Capabilities))
	}
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
	if payload.DeltaRef != "" {
		cache := &types.CapabilityCache{}
		if err := loadJSONFromRef(payload.DeltaRef, cache); err != nil {
			logger.Error("failed to load capability cache delta from ref", "error", err, "ref", payload.DeltaRef)
			return err
		}
		cache.Version = payload.CacheVersion
		cache.SchemaIndexDigest = payload.SchemaIndexDigest
		cache.VerifiedAt = payload.VerifiedAt
		ec.registry.ReplaceCapabilityCache(cache)
		logger.Info("capability cache delta applied", "capabilities", len(cache.Capabilities))
	}
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
	ec.mu.Lock()
	ec.trustRoots = payload
	ec.mu.Unlock()
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

	if payload.PolicyRef != "" {
		policy := &types.MeshPolicy{}
		if err := loadJSONFromRef(payload.PolicyRef, policy); err != nil {
			logger.Error("failed to load policy from ref", "error", err, "ref", payload.PolicyRef)
			return err
		}

		ec.mu.RLock()
		policyEval := ec.policy
		ec.mu.RUnlock()

		if policyEval == nil {
			logger.Warn("policy update received but no evaluator set")
			return nil
		}

		if err := policyEval.UpdatePolicy(ctx, policy); err != nil {
			logger.Error("failed to update policy", "error", err)
			return err
		}
		logger.Info("policy evaluator updated", "version", policy.Version)
	}
	return nil
}

func (ec *EventConsumer) handleIdentityServiceIssued(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.IdentityServiceIssuedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal identity.service.issued payload", "error", err)
		return err
	}

	// Update the service entry in the registry with the new identity
	service := ec.registry.GetService(payload.ServiceID)
	if service != nil {
		service.ServiceIdentity = payload.SpiffeID
		ec.registry.AddOrUpdateService(service)
	}

	// Update any providers associated with this service
	allProviders := ec.registry.ListAllProviders()
	for capID, providers := range allProviders {
		for _, p := range providers {
			if p.ServiceID == payload.ServiceID {
				p.ServiceIdentity = payload.SpiffeID
				ec.registry.AddOrUpdateProvider(capID, p)
			}
		}
	}

	logger.Info("identity issued for service",
		"service_id", payload.ServiceID,
		"spiffe_id", payload.SpiffeID,
		"expires_at", payload.ExpiresAt)
	return nil
}

func (ec *EventConsumer) handleIdentityServiceRevoked(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.IdentityServiceRevokedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal identity.service.revoked payload", "error", err)
		return err
	}

	// Clear the service identity — the service can no longer be used as a provider
	service := ec.registry.GetService(payload.ServiceID)
	if service != nil {
		service.ServiceIdentity = ""
		service.Status = "identity_revoked"
		ec.registry.AddOrUpdateService(service)
	}

	// Mark all providers for this service as unavailable
	allProviders := ec.registry.ListAllProviders()
	for capID, providers := range allProviders {
		for _, p := range providers {
			if p.ServiceID == payload.ServiceID {
				p.Available = false
				p.ServiceIdentity = ""
				ec.registry.AddOrUpdateProvider(capID, p)
			}
		}
	}

	logger.Info("identity revoked for service",
		"service_id", payload.ServiceID,
		"reason", payload.Reason,
		"revoked_at", payload.RevokedAt)
	return nil
}

func (ec *EventConsumer) handleConsentStateUpdated(ctx context.Context, event *types.UAEvent) error {
	logger := logging.GetLogger(ctx)

	payload := &types.ConsentStateUpdatedPayload{}
	if err := unmarshalPayload(event.Payload, payload); err != nil {
		logger.Error("failed to unmarshal consent_state.updated payload", "error", err)
		return err
	}

	ec.mu.RLock()
	policyEval := ec.policy
	ec.mu.RUnlock()

	if policyEval == nil {
		logger.Warn("consent update received but no policy evaluator set")
		return nil
	}

	consented := payload.Decision == "allow" || payload.Decision == "granted"
	if err := policyEval.UpdateConsent(ctx, payload.SubjectRef, payload.CapabilityID, consented); err != nil {
		logger.Error("failed to update consent", "error", err)
		return err
	}

	logger.Info("consent state updated",
		"subject_ref", payload.SubjectRef,
		"capability_id", payload.CapabilityID,
		"decision", payload.Decision)
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

// loadJSONFromRef loads JSON from a file reference (file:// or path).
func loadJSONFromRef(ref string, target interface{}) error {
	path := strings.TrimPrefix(ref, "file://")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
