package binding_engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/policy_evaluator"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/telemetry"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// BindingEngine manages capability binding requests.
type BindingEngine interface {
	// RequestBinding processes a binding request and returns a response.
	RequestBinding(ctx context.Context, req *types.BindingRequest) *types.BindingResponse

	// GetBinding retrieves an active binding by ID.
	GetBinding(ctx context.Context, bindingID string) (*types.BindingStatus, error)

	// RevokeBinding revokes an active binding.
	RevokeBinding(ctx context.Context, bindingID string, reason string) error

	// ListActiveBindings returns all active bindings.
	ListActiveBindings(ctx context.Context) []*types.BindingStatus

	// Start initializes the binding engine.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the binding engine.
	Stop(ctx context.Context) error
}

// Engine implements BindingEngine.
type Engine struct {
	mu               sync.RWMutex
	log              *slog.Logger
	registry         *discovery.Registry
	policyEvaluator  policy_evaluator.PolicyEvaluator
	grants           *GrantManager
	telemetryBuffer  *telemetry.Buffer
	collector        *telemetry.Collector
	expirationTicker *time.Ticker
	stopCh           chan struct{}
}

// NewEngine creates a new binding engine.
func NewEngine(
	log *slog.Logger,
	registry *discovery.Registry,
	policyEvaluator policy_evaluator.PolicyEvaluator,
	telemetryBuffer *telemetry.Buffer,
	collector *telemetry.Collector,
) *Engine {
	// Use configurable expiration check interval
	expirationInterval := ExpirationCheckInterval

	return &Engine{
		log:              log,
		registry:         registry,
		policyEvaluator:  policyEvaluator,
		grants:           NewGrantManager(),
		telemetryBuffer:  telemetryBuffer,
		collector:        collector,
		expirationTicker: time.NewTicker(expirationInterval),
		stopCh:           make(chan struct{}),
	}
}

// RequestBinding implements the full binding resolution flow per RFC §8.1.
func (e *Engine) RequestBinding(ctx context.Context, req *types.BindingRequest) *types.BindingResponse {
	// Input validation
	if req == nil {
		e.log.Error("Nil binding request received")
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonInternalError,
		}
	}
	if req.Client.ServiceID == "" || req.Capability.ID == "" {
		e.log.Error("Invalid binding request: missing required fields",
			"has_client", req.Client.ServiceID != "",
			"has_capability", req.Capability.ID != "")
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonInternalError,
			BindingID:  req.RequestID,
		}
	}

	select {
	case <-ctx.Done():
		e.log.Warn("Binding request cancelled", "error", ctx.Err())
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonInternalError,
			BindingID:  req.RequestID,
		}
	default:
	}

	logger := e.log.With("request_id", req.RequestID, "client", req.Client.ServiceID, "capability", req.Capability.ID)

	// Step 1: Validate capability exists
	capability := e.registry.GetCapability(req.Capability.ID)
	if capability == nil {
		logger.Warn("Capability not found")
		e.emitBindingDenied(req, types.ReasonCapabilityUnknown)
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonCapabilityUnknown,
			BindingID:  req.RequestID,
		}
	}

	// Step 2: Get client service entry
	clientService := e.registry.GetService(req.Client.ServiceID)
	if clientService == nil {
		logger.Warn("Client service not found")
		e.emitBindingDenied(req, types.ReasonNoProviderAvailable)
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonNoProviderAvailable,
			BindingID:  req.RequestID,
		}
	}

	// Step 3: Find candidate providers for this capability
	candidates := e.registry.GetProviders(req.Capability.ID)
	if len(candidates) == 0 {
		logger.Info("No providers available for capability")
		e.emitBindingDenied(req, types.ReasonNoProviderAvailable)
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonNoProviderAvailable,
			BindingID:  req.RequestID,
		}
	}

	// Step 4: Sort by locality preference
	preference := req.Constraints.Locality
	if preference == "" {
		preference = types.LocalityLANPreferred
	}
	sorted := e.registry.SortProvidersByLocality(candidates, clientService.NodeID, preference)
	if len(sorted) == 0 {
		logger.Info("No providers match locality preference")
		e.emitBindingDenied(req, types.ReasonNoProviderAvailable)
		return &types.BindingResponse{
			Decision:   types.BindingDeny,
			ReasonCode: types.ReasonNoProviderAvailable,
			BindingID:  req.RequestID,
		}
	}

	// Step 5: Evaluate each candidate through policy with provider info
	for _, provider := range sorted {
		select {
		case <-ctx.Done():
			logger.Warn("Binding request cancelled", "error", ctx.Err())
			return &types.BindingResponse{
				Decision:   types.BindingDeny,
				ReasonCode: types.ReasonInternalError,
				BindingID:  req.RequestID,
			}
		default:
		}
		logger.Info("Evaluating policy for provider",
			"provider", provider.ServiceID,
			"capability", req.Capability.ID,
			"trust_tier", provider.TrustTier)

		decision, reason, err := e.policyEvaluator.EvaluateBinding(ctx, req, provider)
		if err != nil {
			logger.Error("Policy evaluation error", "error", err, "provider", provider.ServiceID)
			continue
		}

		if decision == types.BindingAllow {
			// Step 6: Issue grant for acceptable provider
			grantReq := &GrantRequest{
				ClientServiceID:   req.Client.ServiceID,
				ClientIdentity:    req.Client.Identity,
				ProviderServiceID: provider.ServiceID,
				ProviderIdentity:  provider.ServiceIdentity,
				CapabilityID:      req.Capability.ID,
				TTLMs:             req.Constraints.TTLMs,
				Constraints:       req.Constraints,
			}

			grant, err := e.grants.IssueGrant(grantReq)
			if err != nil {
				logger.Error("Failed to issue grant", "error", err, "provider", provider.ServiceID)
				e.emitBindingDenied(req, types.ReasonInternalError)
				return &types.BindingResponse{
					Decision:   types.BindingDeny,
					ReasonCode: types.ReasonInternalError,
					BindingID:  req.RequestID,
				}
			}

			logger.Info("Binding approved", "provider", provider.ServiceID)
			e.emitBindingGranted(grant, req, provider)

			return &types.BindingResponse{
				Decision:   types.BindingAllow,
				ReasonCode: "",
				BindingID:  grant.BindingID,
				Provider: &types.ProviderInfo{
					ServiceID: provider.ServiceID,
					Identity:  provider.ServiceIdentity,
					Endpoint:  provider.Endpoint,
				},
				Grant: &types.BindingGrant{
					TokenRef:  grant.BindingID,
					ExpiresAt: grant.ExpiresAt,
					CreatedAt: grant.CreatedAt,
				},
				EnforcedConstraints: req.Constraints,
			}
		}

		if reason != "" {
			logger.Debug("Provider rejected", "provider", provider.ServiceID, "reason", reason)
		}
	}

	// No acceptable provider found
	logger.Info("No acceptable provider found")
	e.emitBindingDenied(req, types.ReasonPolicyDenied)
	return &types.BindingResponse{
		Decision:   types.BindingDeny,
		ReasonCode: types.ReasonPolicyDenied,
		BindingID:  req.RequestID,
	}
}

// emitBindingGranted records a granted binding in telemetry buffer and collector.
func (e *Engine) emitBindingGranted(grant *BindingGrant, req *types.BindingRequest, provider *types.Provider) {
	if e.telemetryBuffer != nil {
		_ = e.telemetryBuffer.RecordBindingGranted(&types.MeshBindingGrantedEvent{
			BindingID:           grant.BindingID,
			ClientServiceID:     req.Client.ServiceID,
			ProviderServiceID:   provider.ServiceID,
			CapabilityID:        req.Capability.ID,
			ExpiresAt:           grant.ExpiresAt,
			EnforcedConstraints: req.Constraints,
			GrantedAt:           grant.CreatedAt,
		})
	}
	if e.collector != nil {
		e.collector.RecordBindingGranted(req.Capability.ID)
	}
}

// emitBindingDenied records a denied binding in telemetry buffer and collector.
func (e *Engine) emitBindingDenied(req *types.BindingRequest, reason types.ReasonCode) {
	if req == nil {
		return
	}
	if e.telemetryBuffer != nil {
		_ = e.telemetryBuffer.RecordBindingDenied(&types.MeshBindingDeniedEvent{
			RequestID:       req.RequestID,
			ClientServiceID: req.Client.ServiceID,
			CapabilityID:    req.Capability.ID,
			ReasonCode:      reason,
			DeniedAt:        time.Now(),
		})
	}
	if e.collector != nil {
		e.collector.RecordBindingDenied(req.Capability.ID, reason)
	}
}

// GetBinding returns a binding by ID.
func (e *Engine) GetBinding(ctx context.Context, bindingID string) (*types.BindingStatus, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	grant, err := e.grants.GetGrant(bindingID)
	if err != nil {
		return nil, err
	}

	// Construct BindingStatus from grant
	status := &types.BindingStatus{
		BindingID:           grant.BindingID,
		State:               grant.State,
		ClientServiceID:     grant.ClientServiceID,
		ProviderServiceID:   grant.ProviderServiceID,
		CapabilityID:        grant.CapabilityID,
		CreatedAt:           grant.CreatedAt,
		ExpiresAt:           grant.ExpiresAt,
		EnforcedConstraints: grant.Constraints,
		LastError:           grant.LastError,
	}

	return status, nil
}

// RevokeBinding revokes a binding.
func (e *Engine) RevokeBinding(ctx context.Context, bindingID string, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.grants.RevokeGrant(bindingID, reason); err != nil {
		return err
	}

	if e.telemetryBuffer != nil {
		_ = e.telemetryBuffer.RecordBindingRevoked(&types.MeshBindingRevokedEvent{
			BindingID: bindingID,
			Reason:    reason,
			RevokedAt: time.Now(),
		})
	}
	if e.collector != nil {
		e.collector.RecordBindingRevoked()
	}

	return nil
}

// ListActiveBindings returns all active bindings.
func (e *Engine) ListActiveBindings(ctx context.Context) []*types.BindingStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.grants.ListActiveGrants()
}

// Start initializes the binding engine and starts background tasks.
func (e *Engine) Start(ctx context.Context) error {
	e.log.Info("Binding engine started")
	go e.monitorGrantExpiration(ctx)
	return nil
}

// Stop gracefully stops the binding engine.
func (e *Engine) Stop(ctx context.Context) error {
	e.log.Info("Binding engine stopped")
	e.expirationTicker.Stop()
	close(e.stopCh)
	return nil
}

// monitorGrantExpiration periodically checks and removes expired grants.
func (e *Engine) monitorGrantExpiration(ctx context.Context) {
	for {
		select {
		case <-e.expirationTicker.C:
			expired := e.grants.ExpireGrants()
			if expired > 0 {
				e.log.Debug("expired grants removed", "count", expired)
			}

		case <-e.stopCh:
			e.log.Debug("grant expiration monitor stopped")
			return

		case <-ctx.Done():
			e.log.Debug("grant expiration monitor stopped")
			return
		}
	}
}

// BindingStatus represents the status of a binding.
type BindingStatus struct {
	BindingID    string             `json:"binding_id"`
	State        types.BindingState `json:"state"`
	ClientID     string             `json:"client_id"`
	ProviderID   string             `json:"provider_id"`
	CapabilityID string             `json:"capability_id"`
	ExpiresAt    time.Time          `json:"expires_at"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

// Ensure there's an alias type for types.BindingStatus if it doesn't exist.
// The types.BindingStatus is defined in types/mesh.go if needed elsewhere.
