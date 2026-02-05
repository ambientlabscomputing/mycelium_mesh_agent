package binding_engine

import (
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// BindingGrant represents an issued capability grant
type BindingGrant struct {
	BindingID         string
	ClientServiceID   string
	ClientIdentity    string
	ProviderServiceID string
	ProviderIdentity  string
	CapabilityID      string
	CreatedAt         time.Time
	ExpiresAt         time.Time
	Constraints       types.BindingConstraints
	RateLimit         *RateLimiter
	State             types.BindingState
	LastError         string
}

// GrantRequest is the input for issuing a grant
type GrantRequest struct {
	ClientServiceID   string
	ClientIdentity    string
	ProviderServiceID string
	ProviderIdentity  string
	CapabilityID      string
	Constraints       types.BindingConstraints
	TTLMs             int
}

// GrantManager manages lifecycle of all binding grants
type GrantManager struct {
	mu     sync.RWMutex
	grants map[string]*BindingGrant
}

// NewGrantManager creates a new grant manager
func NewGrantManager() *GrantManager {
	return &GrantManager{
		grants: make(map[string]*BindingGrant),
	}
}

// IssueGrant creates and stores a new grant
func (gm *GrantManager) IssueGrant(req *GrantRequest) *BindingGrant {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Generate binding ID (UUID)
	bindingID := generateID()

	// Calculate expiration
	ttl := time.Duration(req.TTLMs) * time.Millisecond
	if ttl == 0 {
		ttl = 1 * time.Hour // Default 1 hour
	}
	expiresAt := time.Now().Add(ttl)

	grant := &BindingGrant{
		BindingID:         bindingID,
		ClientServiceID:   req.ClientServiceID,
		ClientIdentity:    req.ClientIdentity,
		ProviderServiceID: req.ProviderServiceID,
		ProviderIdentity:  req.ProviderIdentity,
		CapabilityID:      req.CapabilityID,
		CreatedAt:         time.Now(),
		ExpiresAt:         expiresAt,
		Constraints:       req.Constraints,
		State:             types.BindingStateActive,
	}

	// Create rate limiter if rate limit is configured
	if req.Constraints.RateLimit != nil && req.Constraints.RateLimit.RPS > 0 {
		grant.RateLimit = NewRateLimiter(req.Constraints.RateLimit.RPS, int64(req.Constraints.RateLimit.Burst))
	}

	gm.grants[bindingID] = grant
	return grant
}

// GetGrant returns a grant by ID
func (gm *GrantManager) GetGrant(bindingID string) *BindingGrant {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	grant, _ := gm.grants[bindingID]
	return grant
}

// GetGrantStatus returns status of a grant
func (gm *GrantManager) GetGrantStatus(bindingID string) (*types.BindingStatus, error) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	grant, exists := gm.grants[bindingID]
	if !exists {
		return nil, nil
	}

	return &types.BindingStatus{
		BindingID:           grant.BindingID,
		State:               grant.State,
		ClientServiceID:     grant.ClientServiceID,
		ProviderServiceID:   grant.ProviderServiceID,
		CapabilityID:        grant.CapabilityID,
		CreatedAt:           grant.CreatedAt,
		ExpiresAt:           grant.ExpiresAt,
		EnforcedConstraints: grant.Constraints,
		LastError:           grant.LastError,
	}, nil
}

// RevokeGrant revokes a grant
func (gm *GrantManager) RevokeGrant(bindingID string, reason string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	grant, exists := gm.grants[bindingID]
	if exists {
		grant.State = types.BindingStateRevoked
		grant.LastError = reason
	}
}

// ListActiveGrants returns all active grants
func (gm *GrantManager) ListActiveGrants() []*types.BindingStatus {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	active := make([]*types.BindingStatus, 0)
	now := time.Now()

	for _, grant := range gm.grants {
		if grant.State == types.BindingStateActive {
			// Check if expired
			if now.After(grant.ExpiresAt) {
				grant.State = types.BindingStateExpired
				continue
			}

			status := &types.BindingStatus{
				BindingID:           grant.BindingID,
				State:               grant.State,
				ClientServiceID:     grant.ClientServiceID,
				ProviderServiceID:   grant.ProviderServiceID,
				CapabilityID:        grant.CapabilityID,
				CreatedAt:           grant.CreatedAt,
				ExpiresAt:           grant.ExpiresAt,
				EnforcedConstraints: grant.Constraints,
			}
			active = append(active, status)
		}
	}

	return active
}

// ExpireGrants removes all expired grants
func (gm *GrantManager) ExpireGrants() int {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	expired := 0
	now := time.Now()

	for _, grant := range gm.grants {
		if grant.State == types.BindingStateActive && now.After(grant.ExpiresAt) {
			grant.State = types.BindingStateExpired
			expired++
		}
	}

	// Clean up very old expired grants (older than 24 hours)
	cutoff := now.Add(-24 * time.Hour)
	for id, grant := range gm.grants {
		if grant.State == types.BindingStateExpired && grant.ExpiresAt.Before(cutoff) {
			delete(gm.grants, id)
		}
	}

	return expired
}

// CheckRateLimit checks if a request respects rate limit
func (g *BindingGrant) CheckRateLimit() bool {
	if g.RateLimit == nil {
		return true
	}
	return g.RateLimit.Allow()
}

// ===== Rate Limiter =====

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	rps    float64
	burst  int64
	tokens float64
	mu     sync.Mutex
	last   time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rps float64, burst int64) *RateLimiter {
	return &RateLimiter{
		rps:    rps,
		burst:  burst,
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// Allow checks if a request is allowed under rate limit
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.tokens = min(float64(rl.burst), rl.tokens+elapsed*rl.rps)
	rl.last = now

	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}

	return false
}

// Helper function for minimum
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Helper to generate unique binding IDs
// TODO: Replace with proper UUID generation
func generateID() string {
	return "binding-" + time.Now().Format("20060102150405") + "-" + randomString(8)
}

// randomString generates a random string for IDs
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[int(time.Now().UnixNano())%len(charset)]
	}
	return string(b)
}
