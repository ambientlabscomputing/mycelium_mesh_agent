package discovery

import (
	"sync"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Registry manages in-memory index of services, providers, and capabilities.
type Registry struct {
	mu        sync.RWMutex
	members   map[string]*types.MemberInfo   // nodeID -> MemberInfo
	services  map[string]*types.ServiceEntry // serviceID -> ServiceEntry
	providers map[string][]*types.Provider   // capabilityID -> []*Provider
	cache     *types.CapabilityCache
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		members:   make(map[string]*types.MemberInfo),
		services:  make(map[string]*types.ServiceEntry),
		providers: make(map[string][]*types.Provider),
		cache: &types.CapabilityCache{
			Capabilities: make(map[string]*types.Capability),
		},
	}
}

// AddOrUpdateMember adds or updates a cluster member.
func (r *Registry) AddOrUpdateMember(member *types.MemberInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[member.NodeID] = member
}

// RemoveMember removes a cluster member.
func (r *Registry) RemoveMember(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.members, nodeID)
}

// GetMember returns a member by node ID.
func (r *Registry) GetMember(nodeID string) *types.MemberInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.members[nodeID]
}

// ListMembers returns all cluster members.
func (r *Registry) ListMembers() []*types.MemberInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*types.MemberInfo, 0, len(r.members))
	for _, m := range r.members {
		result = append(result, m)
	}
	return result
}

// AddOrUpdateService adds or updates a service in the registry.
func (r *Registry) AddOrUpdateService(service *types.ServiceEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[service.ServiceID] = service
}

// RemoveService removes a service from the registry.
func (r *Registry) RemoveService(serviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, serviceID)
}

// GetService returns a service by ID.
func (r *Registry) GetService(serviceID string) *types.ServiceEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.services[serviceID]
}

// ListServices returns all services.
func (r *Registry) ListServices() []*types.ServiceEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*types.ServiceEntry, 0, len(r.services))
	for _, s := range r.services {
		result = append(result, s)
	}
	return result
}

// AddOrUpdateProvider adds or updates a provider for a capability.
func (r *Registry) AddOrUpdateProvider(capabilityID string, provider *types.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()

	providers, exists := r.providers[capabilityID]
	if !exists {
		r.providers[capabilityID] = []*types.Provider{provider}
		return
	}

	// Update existing or append
	found := false
	for i, p := range providers {
		if p.ServiceID == provider.ServiceID {
			providers[i] = provider
			found = true
			break
		}
	}
	if !found {
		r.providers[capabilityID] = append(providers, provider)
	}
}

// RemoveProvider removes a provider.
func (r *Registry) RemoveProvider(capabilityID string, serviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	providers, exists := r.providers[capabilityID]
	if !exists {
		return
	}

	filtered := make([]*types.Provider, 0, len(providers))
	for _, p := range providers {
		if p.ServiceID != serviceID {
			filtered = append(filtered, p)
		}
	}
	r.providers[capabilityID] = filtered
}

// GetProviders returns all providers for a capability.
func (r *Registry) GetProviders(capabilityID string) []*types.Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providers, exists := r.providers[capabilityID]
	if !exists {
		return []*types.Provider{}
	}

	result := make([]*types.Provider, len(providers))
	copy(result, providers)
	return result
}

// ListAllProviders returns all providers across all capabilities.
func (r *Registry) ListAllProviders() map[string][]*types.Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string][]*types.Provider)
	for k, v := range r.providers {
		providers := make([]*types.Provider, len(v))
		copy(providers, v)
		result[k] = providers
	}
	return result
}

// AddOrUpdateCapability adds or updates a capability in cache.
func (r *Registry) AddOrUpdateCapability(cap *types.Capability) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache.Capabilities[cap.ID] = cap
}

// GetCapability returns a capability from cache.
func (r *Registry) GetCapability(capabilityID string) *types.Capability {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache.Capabilities[capabilityID]
}

// ListCapabilities returns all capabilities in cache.
func (r *Registry) ListCapabilities() []*types.Capability {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*types.Capability, 0, len(r.cache.Capabilities))
	for _, c := range r.cache.Capabilities {
		result = append(result, c)
	}
	return result
}

// CalculateProviderDistance calculates distance between client and provider nodes.
func (r *Registry) CalculateProviderDistance(clientNodeID string, providerNodeID string) types.ProviderDistance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if clientNodeID == providerNodeID {
		return types.DistanceSameNode
	}

	clientMember := r.members[clientNodeID]
	providerMember := r.members[providerNodeID]

	if clientMember == nil || providerMember == nil {
		// Unknown member, assume WAN
		return types.DistanceWAN
	}

	// Same site/datacenter (use tags for now)
	if len(clientMember.Tags) > 0 && len(providerMember.Tags) > 0 {
		if clientMember.Tags["site"] == providerMember.Tags["site"] {
			return types.DistanceSameLAN
		}
	}

	// Different sites
	return types.DistanceWAN
}

// SortProvidersByLocality sorts providers by locality preference.
func (r *Registry) SortProvidersByLocality(providers []*types.Provider, clientNodeID string, preference types.LocalityPreference) []*types.Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create distance scores for each provider
	type providerScore struct {
		provider *types.Provider
		distance types.ProviderDistance
		score    int
	}

	scores := make([]providerScore, 0, len(providers))
	for _, p := range providers {
		distance := r.CalculateProviderDistance(clientNodeID, p.NodeID)
		ps := providerScore{
			provider: p,
			distance: distance,
		}

		// Score based on preference (higher = better)
		switch distance {
		case types.DistanceSameNode:
			ps.score = 3
		case types.DistanceSameLAN:
			ps.score = 2
		case types.DistanceWAN:
			ps.score = 1
		default:
			ps.score = 0
		}

		// Adjust based on locality preference
		switch preference {
		case types.LocalityNodeOnly:
			if distance != types.DistanceSameNode {
				ps.score = 0 // Not eligible
			}
		case types.LocalityLANOnly:
			if distance == types.DistanceWAN {
				ps.score = 0 // Not eligible
			}
		case types.LocalityLANPreferred:
			// Boost LAN, allow WAN
			if distance == types.DistanceWAN {
				ps.score = 1
			}
		case types.LocalityWANAllowed:
			// All allowed with preference
			// Already scored
		}

		scores = append(scores, ps)
	}

	// Simple bubble sort by score (descending)
	for i := 0; i < len(scores); i++ {
		for j := i + 1; j < len(scores); j++ {
			if scores[j].score > scores[i].score {
				scores[i], scores[j] = scores[j], scores[i]
			}
		}
	}

	// Build result preserving only eligible providers
	result := make([]*types.Provider, 0, len(providers))
	for _, ps := range scores {
		if ps.score > 0 {
			result = append(result, ps.provider)
		}
	}

	return result
}

// GetCapabilityCache returns the underlying capability cache.
func (r *Registry) GetCapabilityCache() *types.CapabilityCache {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache
}

// Clear removes all data from registry.
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.members = make(map[string]*types.MemberInfo)
	r.services = make(map[string]*types.ServiceEntry)
	r.providers = make(map[string][]*types.Provider)
}
