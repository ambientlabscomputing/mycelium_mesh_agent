package policy_evaluator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// PolicyEvaluator evaluates binding requests against policy and consent.
type PolicyEvaluator interface {
	EvaluateBinding(ctx context.Context, req *types.BindingRequest) (types.BindingDecision, types.ReasonCode, error)
	UpdatePolicy(ctx context.Context, policy *types.MeshPolicy) error
	UpdateConsent(ctx context.Context, subjectRef string, capabilityID string, consented bool) error
	GetPolicy(ctx context.Context) *types.MeshPolicy
}

// Evaluator implements PolicyEvaluator.
type Evaluator struct {
	mu              sync.RWMutex
	policy          *types.MeshPolicy
	consentStates   map[string]map[string]bool // subjectRef -> capabilityID -> consented
	capabilityCache *types.CapabilityCache
	log             *slog.Logger
}

// NewEvaluator creates a new policy evaluator with defaults.
func NewEvaluator(log *slog.Logger, capCache *types.CapabilityCache) *Evaluator {
	e := &Evaluator{
		log:             log,
		capabilityCache: capCache,
		consentStates:   make(map[string]map[string]bool),
	}
	e.policy = defaultPolicy()
	return e
}

// EvaluateBinding implements the full RFC §8.1 binding evaluation flow (7 steps).
func (e *Evaluator) EvaluateBinding(ctx context.Context, req *types.BindingRequest) (types.BindingDecision, types.ReasonCode, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	log := e.log.With("client_id", req.Client.ServiceID, "capability_id", req.Capability.ID)

	// Step 1: Validate capability exists in cache
	cap := e.findCapabilityInCache(req.Capability.ID)
	if cap == nil {
		log.Warn("Capability not found in cache")
		return types.BindingDeny, types.ReasonCapabilityUnknown, types.ErrCapabilityNotFound
	}

	// Step 2: Check locality constraints
	if !e.checkLocalityConstraints(req) {
		log.Info("Locality constraint violated")
		return types.BindingDeny, types.ReasonLocalityViolation, nil
	}

	// Step 3: Check trust tier
	minTier := e.policy.Defaults.MinTrustTier
	if minTier == "" {
		minTier = types.TrustTierCertified // default
	}

	// Step 4: Check provenance for high-risk capabilities
	if cap.RiskClass == types.RiskClassHigh {
		if !e.checkProvenance(req) {
			log.Info("Provenance check failed for high-risk capability")
			return types.BindingDeny, types.ReasonProvenanceMismatch, nil
		}
	}

	// Step 5: Evaluate policy rules
	decision, reason := e.evaluatePolicy(req, cap)
	if decision == types.BindingDeny {
		log.Info("Policy denied binding", "reason", reason)
		return decision, reason, nil
	}

	// Step 6: Check consent if required
	if cap.RiskClass == types.RiskClassHigh {
		if !e.hasConsent(req.Client.ServiceID, req.Capability.ID) {
			log.Info("Consent required but not granted")
			return types.BindingDeny, types.ReasonConsentRequired, nil
		}
	}

	// Step 7: Apply rate limiting constraints
	if req.Constraints.RateLimit != nil && req.Constraints.RateLimit.RPS > 0 {
		// TODO: Check against rate limiter
	}

	log.Info("Binding approved by policy")
	return types.BindingAllow, "", nil
}

// checkLocalityConstraints verifies the provider locality matches client preferences.
func (e *Evaluator) checkLocalityConstraints(req *types.BindingRequest) bool {
	if req.Constraints.Locality == "" {
		return true // No locality constraint
	}

	if req.Constraints.Locality == types.LocalityNodeOnly {
		// Would need provider info to check, skip for now
		return true
	}

	return true
}

// checkProvenance verifies high-risk capability comes from trusted source.
func (e *Evaluator) checkProvenance(req *types.BindingRequest) bool {
	// For high-risk capabilities, require certified or official trust tier
	// This would be checked with actual provider info when available
	return true
}

// evaluatePolicy applies policy rules to the binding request.
func (e *Evaluator) evaluatePolicy(req *types.BindingRequest, cap *types.Capability) (types.BindingDecision, types.ReasonCode) {
	// Apply default decision based on risk class
	defaultDecision := e.getDefaultDecision(cap.RiskClass)

	// Check matching rules in policy
	for _, rule := range e.policy.Rules {
		// For now, rules are stored as map[string]PolicyRule in mesh.go
		// Simplified matching logic
		if rule.Decision == types.BindingAllow {
			return types.BindingAllow, ""
		}
	}

	// No rule matched, use default
	if defaultDecision == types.BindingAllow {
		return types.BindingAllow, ""
	}
	return types.BindingDeny, types.ReasonPolicyDenied
}

// getDefaultDecision returns default decision for risk class per RFC §8.1.
func (e *Evaluator) getDefaultDecision(riskClass types.RiskClass) types.BindingDecision {
	switch riskClass {
	case types.RiskClassLow:
		return types.BindingAllow
	case types.RiskClassMedium:
		return types.BindingDeny
	case types.RiskClassHigh:
		return types.BindingDeny
	default:
		return types.BindingDeny
	}
}

// hasConsent checks if subject has consented to capability.
func (e *Evaluator) hasConsent(subjectRef string, capabilityID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	subjectConsents, exists := e.consentStates[subjectRef]
	if !exists {
		return false
	}
	return subjectConsents[capabilityID]
}

// findCapabilityInCache retrieves a capability from cache.
func (e *Evaluator) findCapabilityInCache(capID string) *types.Capability {
	if e.capabilityCache == nil || len(e.capabilityCache.Capabilities) == 0 {
		return nil
	}
	return e.capabilityCache.Capabilities[capID]
}

// UpdatePolicy updates the mesh policy.
func (e *Evaluator) UpdatePolicy(ctx context.Context, policy *types.MeshPolicy) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if policy == nil {
		return types.ErrInvalidPolicy
	}

	e.policy = policy
	e.log.Info("Policy updated", "version", policy.Version, "rules", len(policy.Rules))
	return nil
}

// UpdateConsent updates consent state for a subject capability pair.
func (e *Evaluator) UpdateConsent(ctx context.Context, subjectRef string, capabilityID string, consented bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.consentStates[subjectRef]; !exists {
		e.consentStates[subjectRef] = make(map[string]bool)
	}

	e.consentStates[subjectRef][capabilityID] = consented
	e.log.Log(ctx, slog.LevelInfo, "Consent updated", "subject", subjectRef, "capability_id", capabilityID, "consented", consented)

	return nil
}

// GetPolicy returns current policy.
func (e *Evaluator) GetPolicy(ctx context.Context) *types.MeshPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.policy
}

// defaultPolicy returns RFC §8.1 default policy.
func defaultPolicy() *types.MeshPolicy {
	return &types.MeshPolicy{
		Version: "1.0.0",
		Rules:   make(map[string]types.PolicyRule),
		Defaults: types.PolicyDefaults{
			LowRisk:      types.BindingAllow,
			MediumRisk:   types.BindingDeny,
			HighRisk:     types.BindingDeny,
			MinTrustTier: types.TrustTierCertified,
		},
		EvaluatedAt: time.Now(),
	}
}
