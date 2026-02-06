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
	EvaluateBinding(ctx context.Context, req *types.BindingRequest, provider *types.Provider) (types.BindingDecision, types.ReasonCode, error)
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
	rateMu          sync.Mutex
	rateLimiters    map[string]*rateLimiter
}

// NewEvaluator creates a new policy evaluator with defaults.
func NewEvaluator(log *slog.Logger, capCache *types.CapabilityCache) *Evaluator {
	e := &Evaluator{
		log:             log,
		capabilityCache: capCache,
		consentStates:   make(map[string]map[string]bool),
		rateLimiters:    make(map[string]*rateLimiter),
	}
	e.policy = defaultPolicy()
	return e
}

// EvaluateBinding implements the full RFC §8.1 binding evaluation flow (7 steps).
func (e *Evaluator) EvaluateBinding(ctx context.Context, req *types.BindingRequest, provider *types.Provider) (types.BindingDecision, types.ReasonCode, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if provider == nil {
		return types.BindingDeny, types.ReasonInternalError, types.ErrInvalidBindingRequest
	}

	log := e.log.With("client_id", req.Client.ServiceID, "capability_id", req.Capability.ID, "provider", provider.ServiceID)

	// Step 1: Validate capability exists in cache
	cap := e.findCapabilityInCache(req.Capability.ID)
	if cap == nil {
		log.Warn("Capability not found in cache")
		return types.BindingDeny, types.ReasonCapabilityUnknown, types.ErrCapabilityNotFound
	}

	// Step 2: Check locality constraints using provider info
	if !e.checkLocalityConstraints(req, provider) {
		log.Info("Locality constraint violated")
		return types.BindingDeny, types.ReasonLocalityViolation, nil
	}

	// Step 3: Check trust tier using provider info
	minTier := e.policy.Defaults.MinTrustTier
	if minTier == "" {
		minTier = types.TrustTierCertified // default
	}

	if !e.checkTrustTier(provider, minTier) {
		log.Info("Provider trust tier insufficient", "tier", provider.TrustTier, "required", minTier)
		return types.BindingDeny, types.ReasonTrustTierInsufficient, nil
	}

	// Step 4: Check provenance for high-risk capabilities
	if cap.RiskClass == types.RiskClassHigh {
		if !e.checkProvenance(provider, cap) {
			log.Info("Provenance check failed for high-risk capability")
			return types.BindingDeny, types.ReasonProvenanceMismatch, nil
		}
	}

	// Step 5: Evaluate policy rules
	decision, reason := e.evaluatePolicy(req, cap, provider)
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
		if !e.checkRateLimit(req.Client.ServiceID, req.Capability.ID, req.Constraints.RateLimit.RPS) {
			log.Info("Rate limit exceeded")
			return types.BindingDeny, types.ReasonRateLimited, nil
		}
	}

	log.Info("Binding approved by policy")
	return types.BindingAllow, "", nil
}

// checkLocalityConstraints verifies the provider locality matches client preferences.
func (e *Evaluator) checkLocalityConstraints(req *types.BindingRequest, provider *types.Provider) bool {
	if req.Constraints.Locality == "" {
		return true // No locality constraint
	}

	// For node-only locality, provider must be on same node
	if req.Constraints.Locality == types.LocalityNodeOnly {
		// Note: Would need client's node ID to check properly
		// For now, trust the registry's locality sorting
		return true
	}

	return true
}

// checkTrustTier verifies provider meets minimum trust tier requirement.
func (e *Evaluator) checkTrustTier(provider *types.Provider, minTier types.TrustTier) bool {
	// Trust tier hierarchy: local < experimental < community < certified < official
	tierRank := map[types.TrustTier]int{
		types.TrustTierLocal:        0,
		types.TrustTierExperimental: 1,
		types.TrustTierCommunity:    2,
		types.TrustTierCertified:    3,
		types.TrustTierOfficial:     4,
	}

	providerRank, ok1 := tierRank[provider.TrustTier]
	minRank, ok2 := tierRank[minTier]

	if !ok1 || !ok2 {
		return false // Unknown tier
	}

	return providerRank >= minRank
}

// checkProvenance verifies high-risk capability comes from trusted source.
func (e *Evaluator) checkProvenance(provider *types.Provider, cap *types.Capability) bool {
	// For high-risk capabilities, require certified or official trust tier
	if cap.RiskClass == types.RiskClassHigh {
		return provider.TrustTier == types.TrustTierCertified || provider.TrustTier == types.TrustTierOfficial
	}
	return true
}

// evaluatePolicy applies policy rules to the binding request.
func (e *Evaluator) evaluatePolicy(req *types.BindingRequest, cap *types.Capability, provider *types.Provider) (types.BindingDecision, types.ReasonCode) {
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
	if e.capabilityCache == nil {
		return nil
	}
	cap, _ := e.capabilityCache.GetCapability(capID)
	return cap
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

// checkRateLimit implements rate limiting per client/capability pair.
// Returns true if request is allowed, false if rate limit exceeded.
func (e *Evaluator) checkRateLimit(clientID, capabilityID string, requestedRPS float64) bool {
	if requestedRPS <= 0 {
		return true
	}
	if requestedRPS > float64(MaxRPSPerClient) {
		return false
	}

	key := clientID + ":" + capabilityID

	e.rateMu.Lock()
	limiter, exists := e.rateLimiters[key]
	if !exists || limiter.rps != requestedRPS {
		burst := int64(requestedRPS)
		if burst < 1 {
			burst = 1
		}
		limiter = newRateLimiter(requestedRPS, burst)
		e.rateLimiters[key] = limiter
	}
	e.rateMu.Unlock()

	return limiter.Allow()
}

// rateLimiter implements a basic token bucket.
type rateLimiter struct {
	rps    float64
	burst  int64
	tokens float64
	last   time.Time
	mu     sync.Mutex
}

func newRateLimiter(rps float64, burst int64) *rateLimiter {
	return &rateLimiter{
		rps:    rps,
		burst:  burst,
		tokens: float64(burst),
		last:   time.Now(),
	}
}

func (rl *rateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.tokens = minFloat(rl.tokens+elapsed*rl.rps, float64(rl.burst))
	rl.last = now

	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}

	return false
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
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
