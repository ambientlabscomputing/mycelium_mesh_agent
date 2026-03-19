package types

import (
	"sync"
	"time"
)

// ===== UA Events =====

// UAEvent is the envelope for all events from the Underleaf Agent.
type UAEvent struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	EmittedAt time.Time   `json:"emitted_at"`
	ClusterID string      `json:"cluster_id"`
	NodeID    string      `json:"node_id"`
	Seq       uint64      `json:"seq"`
	EntityRef *EntityRef  `json:"entity_ref,omitempty"`
	Payload   interface{} `json:"payload"`
	Signature string      `json:"signature,omitempty"`
}

// EntityRef identifies the entity that the event is about
type EntityRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ===== Membership Events =====

type ClusterSnapshotPayload struct {
	Members              []MemberInfo `json:"members"`
	ClusterCAFingerprint string       `json:"cluster_ca_fingerprint"`
	MeshConfigVersion    string       `json:"mesh_config_version"`
}

type MemberInfo struct {
	NodeID       string            `json:"node_id"`
	NodeIdentity string            `json:"node_identity"`
	Endpoints    []Endpoint        `json:"endpoints"`
	Tags         map[string]string `json:"tags,omitempty"`
	Status       string            `json:"status"`
}

type Endpoint struct {
	Proto string            `json:"proto"`
	Host  string            `json:"host"`
	Port  int               `json:"port"`
	Scope string            `json:"scope"`
	Tags  map[string]string `json:"tags,omitempty"`
}

type MemberJoinedPayload struct {
	NodeID       string            `json:"node_id"`
	NodeIdentity string            `json:"node_identity"`
	Endpoints    []Endpoint        `json:"endpoints"`
	Tags         map[string]string `json:"tags,omitempty"`
}

type MemberUpdatedPayload struct {
	NodeID    string            `json:"node_id"`
	Endpoints []Endpoint        `json:"endpoints,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Status    string            `json:"status,omitempty"`
}

type MemberLeftPayload struct {
	NodeID string `json:"node_id"`
	Reason string `json:"reason"`
}

type MemberHealthPayload struct {
	NodeID   string    `json:"node_id"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

// ===== Identity Events =====

type IdentityTrustRootsUpdatedPayload struct {
	ClusterTrustBundle string    `json:"cluster_trust_bundle"`
	ValidFrom          time.Time `json:"valid_from"`
	ValidTo            time.Time `json:"valid_to"`
	RotationID         string    `json:"rotation_id"`
}

type IdentityServiceIssuedPayload struct {
	ServiceID string            `json:"service_id"`
	SpiffeID  string            `json:"spiffe_id"`
	CertRef   string            `json:"cert_ref"`
	Claims    map[string]string `json:"claims"`
	ExpiresAt time.Time         `json:"expires_at"`
}

type IdentityServiceRevokedPayload struct {
	ServiceID string    `json:"service_id"`
	Reason    string    `json:"reason"`
	RevokedAt time.Time `json:"revoked_at"`
}

// ===== Service Events =====

type ServiceStartedPayload struct {
	ServiceID            string            `json:"service_id"`
	ServiceIdentity      string            `json:"service_identity"`
	NodeID               string            `json:"node_id"`
	Endpoints            []Endpoint        `json:"endpoints"`
	CapabilitiesProvided []CapabilityRef   `json:"capabilities_provided,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
}

type ServiceUpdatedPayload struct {
	ServiceID            string            `json:"service_id"`
	Endpoints            []Endpoint        `json:"endpoints,omitempty"`
	CapabilitiesProvided []CapabilityRef   `json:"capabilities_provided,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	RollingState         string            `json:"rolling_state,omitempty"`
}

type ServiceStoppedPayload struct {
	ServiceID string `json:"service_id"`
	Reason    string `json:"reason"`
}

type CapabilityRef struct {
	CapabilityID string `json:"capability_id"`
	Version      string `json:"version"`
}

// ===== Capability Cache Events =====

type CapabilityCacheSnapshotUpdatedPayload struct {
	CacheVersion      string    `json:"cache_version"`
	SignedSnapshotRef string    `json:"signed_snapshot_ref"`
	VerifiedAt        time.Time `json:"verified_at"`
	SchemaIndexDigest string    `json:"schema_index_digest"`
}

type CapabilityCacheDeltaUpdatedPayload struct {
	CacheVersion      string    `json:"cache_version"`
	DeltaRef          string    `json:"delta_ref"`
	VerifiedAt        time.Time `json:"verified_at"`
	SchemaIndexDigest string    `json:"schema_index_digest"`
}

// ===== Policy Events =====

type MeshPolicyUpdatedPayload struct {
	PolicyVersion string    `json:"policy_version"`
	PolicyRef     string    `json:"policy_ref"`
	EffectiveAt   time.Time `json:"effective_at"`
}

type ConsentStateUpdatedPayload struct {
	SubjectRef   string                 `json:"subject_ref"`
	CapabilityID string                 `json:"capability_id"`
	Decision     string                 `json:"decision"`
	Constraints  map[string]interface{} `json:"constraints"`
	Version      string                 `json:"version"`
}

// ===== Exposure Events =====

type ExposureBindRequestedPayload struct {
	ExposureID       string `json:"exposure_id"`
	LeaseID          string `json:"lease_id"`
	Hostname         string `json:"hostname"`
	TargetPort       int    `json:"target_port"`
	LocalAddr        string `json:"local_addr"`         // resolved by agent from deployment service ports
	HyphaeTunnelAddr string `json:"hyphae_tunnel_addr"` // tunnel server addr forwarded from server_api; overrides static agent config
}

type ExposureBindCompletedPayload struct {
	ExposureID string `json:"exposure_id"`
	LeaseID    string `json:"lease_id"`
	Status     string `json:"status"` // "bound" or "error"
	PublicURL  string `json:"public_url,omitempty"`
	Error      string `json:"error,omitempty"`
}

type ExposureUnbindRequestedPayload struct {
	ExposureID string `json:"exposure_id"`
	LeaseID    string `json:"lease_id"`
}

type ExposureUnbindCompletedPayload struct {
	ExposureID string `json:"exposure_id"`
	Error      string `json:"error,omitempty"`
}

// ===== Tunnel Events =====

type TunnelBindRequestedPayload struct {
	TunnelID         string `json:"tunnel_id"`
	LeaseID          string `json:"lease_id"`
	Hostname         string `json:"hostname"`
	Target           string `json:"target"`      // port number or URL
	TargetType       string `json:"target_type"` // "port" or "url"
	HyphaeTunnelAddr string `json:"hyphae_tunnel_addr"`
}

type TunnelBindCompletedPayload struct {
	TunnelID  string `json:"tunnel_id"`
	LeaseID   string `json:"lease_id"`
	Status    string `json:"status"` // "bound" or "error"
	PublicURL string `json:"public_url,omitempty"`
	Error     string `json:"error,omitempty"`
}

type TunnelUnbindRequestedPayload struct {
	TunnelID string `json:"tunnel_id"`
	LeaseID  string `json:"lease_id"`
}

// ===== Channel Events (UNDF-111) =====

// ChannelBindRequestedPayload is the Spine event payload forwarded to MMA
// when server_api publishes a channel.bind.request event.
type ChannelBindRequestedPayload struct {
	ChannelID        string `json:"channel_id"`
	OrgID            string `json:"org_id"`
	Role             string `json:"role"`              // "listener" | "initiator"
	Grant            string `json:"grant,omitempty"`   // ES256 JWT; only for "initiator"
	SourceServerID   string `json:"source_server_id"`
	DestServerID     string `json:"dest_server_id"`
	Purpose          string `json:"purpose,omitempty"`
	HyphaeTunnelAddr string `json:"hyphae_tunnel_addr"`
	ExpiresAt        int64  `json:"expires_at"`
	CreatedAt        int64  `json:"created_at"`
}

// ChannelBindCompletedPayload is emitted by MMA after a channel bind attempt.
type ChannelBindCompletedPayload struct {
	ChannelID string `json:"channel_id"`
	Role      string `json:"role"`
	Status    string `json:"status"` // "active" or "error"
	Error     string `json:"error,omitempty"`
}

// ===== Event Type Constants =====

const (
	EventClusterSnapshot                = "cluster.snapshot"
	EventMemberJoined                   = "member.joined"
	EventMemberUpdated                  = "member.updated"
	EventMemberLeft                     = "member.left"
	EventMemberHealth                   = "member.health"
	EventIdentityTrustRootsUpdated      = "identity.trust_roots.updated"
	EventIdentityServiceIssued          = "identity.service.issued"
	EventIdentityServiceRevoked         = "identity.service.revoked"
	EventServiceStarted                 = "service.started"
	EventServiceUpdated                 = "service.updated"
	EventServiceStopped                 = "service.stopped"
	EventCapabilityCacheSnapshotUpdated = "capability_cache.snapshot.updated"
	EventCapabilityCacheDeltaUpdated    = "capability_cache.delta.updated"
	EventMeshPolicyUpdated              = "mesh_policy.updated"
	EventConsentStateUpdated            = "consent_state.updated"
	EventExposureBindRequested          = "exposure.bind.requested"
	EventExposureBindCompleted          = "exposure.bind.completed"
	EventExposureUnbindRequested        = "exposure.unbind.requested"
	EventExposureUnbindCompleted        = "exposure.unbind.completed"
	EventTunnelBindRequested            = "tunnel.bind.requested"
	EventTunnelBindCompleted            = "tunnel.bind.completed"
	EventTunnelUnbindRequested          = "tunnel.unbind.requested"
	EventTunnelUnbindCompleted          = "tunnel.unbind.completed"

	EventChannelBindRequested  = "channel.bind.requested"  // server_api requests agent to bind a relay channel
	EventChannelBindCompleted  = "channel.bind.completed"  // agent reports bind success/failure
)

// ===== Binding Types =====

// BindingRequest is the request to establish a capability binding
type BindingRequest struct {
	RequestID   string               `json:"request_id" binding:"required"`
	Client      ClientRef            `json:"client" binding:"required"`
	Capability  CapabilityConstraint `json:"capability" binding:"required"`
	Constraints BindingConstraints   `json:"constraints"`
	Context     BindingContext       `json:"context"`
}

// ClientRef identifies the requesting client service
type ClientRef struct {
	ServiceID string `json:"service_id" binding:"required"`
	Identity  string `json:"identity" binding:"required"`
}

// CapabilityConstraint specifies which capability to bind
type CapabilityConstraint struct {
	ID                 string `json:"id" binding:"required"`
	VersionConstraint  string `json:"version_constraint"`
	RequireDigestMatch bool   `json:"require_digest_match,omitempty"`
}

// BindingConstraints specify runtime constraints on the binding
type BindingConstraints struct {
	Locality     LocalityPreference `json:"locality,omitempty"`
	MaxLatencyMs int                `json:"max_latency_ms,omitempty"`
	MinTrustTier TrustTier          `json:"min_trust_tier,omitempty"`
	TTLMs        int                `json:"ttl_ms,omitempty"`
	RateLimit    *RateLimitConfig   `json:"rate_limit,omitempty"`
}

// RateLimitConfig specifies rate limiting
type RateLimitConfig struct {
	RPS   float64 `json:"rps,omitempty"`
	Burst int     `json:"burst,omitempty"`
}

// BindingContext provides additional context
type BindingContext struct {
	UserPresent bool   `json:"user_present,omitempty"`
	Purpose     string `json:"purpose,omitempty"`
}

// BindingResponse is the response to a binding request
type BindingResponse struct {
	Decision            BindingDecision    `json:"decision"`
	ReasonCode          ReasonCode         `json:"reason_code"`
	BindingID           string             `json:"binding_id,omitempty"`
	Provider            *ProviderInfo      `json:"provider,omitempty"`
	Grant               *BindingGrant      `json:"grant,omitempty"`
	EnforcedConstraints BindingConstraints `json:"enforced_constraints,omitempty"`
	PolicyTraceRef      string             `json:"policy_trace_ref,omitempty"`
}

// ProviderInfo identifies the provider service
type ProviderInfo struct {
	ServiceID string   `json:"service_id"`
	Identity  string   `json:"identity"`
	Endpoint  Endpoint `json:"endpoint"`
}

// BindingGrant represents an issued capability grant
type BindingGrant struct {
	TokenRef       string    `json:"token_ref,omitempty"`
	MTLSContextRef string    `json:"mtls_context_ref,omitempty"`
	ExpiresAt      time.Time `json:"expires_at"`
	CreatedAt      time.Time `json:"created_at"`
}

// BindingStatus represents the current state of a binding
type BindingStatus struct {
	BindingID           string             `json:"binding_id"`
	State               BindingState       `json:"state"`
	ClientServiceID     string             `json:"client_service_id"`
	ProviderServiceID   string             `json:"provider_service_id"`
	CapabilityID        string             `json:"capability_id"`
	CreatedAt           time.Time          `json:"created_at"`
	ExpiresAt           time.Time          `json:"expires_at"`
	EnforcedConstraints BindingConstraints `json:"enforced_constraints"`
	LastError           string             `json:"last_error,omitempty"`
}

// ===== Enums =====

type BindingDecision string

const (
	BindingAllow BindingDecision = "allow"
	BindingDeny  BindingDecision = "deny"
)

type BindingState string

const (
	BindingStateActive  BindingState = "active"
	BindingStateExpired BindingState = "expired"
	BindingStateRevoked BindingState = "revoked"
	BindingStateFailed  BindingState = "failed"
)

type ReasonCode string

const (
	ReasonCapabilityUnknown     ReasonCode = "CAPABILITY_UNKNOWN"
	ReasonNoProviderAvailable   ReasonCode = "NO_PROVIDER_AVAILABLE"
	ReasonPolicyDenied          ReasonCode = "POLICY_DENIED"
	ReasonConsentRequired       ReasonCode = "CONSENT_REQUIRED"
	ReasonTrustTierInsufficient ReasonCode = "TRUST_TIER_INSUFFICIENT"
	ReasonProvenanceMismatch    ReasonCode = "PROVENANCE_MISMATCH"
	ReasonLocalityViolation     ReasonCode = "LOCALITY_VIOLATION"
	ReasonRateLimited           ReasonCode = "RATE_LIMITED"
	ReasonInternalError         ReasonCode = "INTERNAL_ERROR"
)

type LocalityPreference string

const (
	LocalityNodeOnly     LocalityPreference = "node_only"
	LocalityLANPreferred LocalityPreference = "lan_preferred"
	LocalityLANOnly      LocalityPreference = "lan_only"
	LocalityWANAllowed   LocalityPreference = "wan_allowed"
)

type TrustTier string

const (
	TrustTierOfficial     TrustTier = "official"
	TrustTierCertified    TrustTier = "certified"
	TrustTierCommunity    TrustTier = "community"
	TrustTierExperimental TrustTier = "experimental"
	TrustTierLocal        TrustTier = "local"
)

type RiskClass string

const (
	RiskClassLow    RiskClass = "low"
	RiskClassMedium RiskClass = "medium"
	RiskClassHigh   RiskClass = "high"
)

// ===== Mesh Types =====

type MeshState string

const (
	MeshStateReady    MeshState = "ready"
	MeshStateDegraded MeshState = "degraded"
	MeshStateOffline  MeshState = "offline"
)

type ServiceEntry struct {
	ServiceID            string            `json:"service_id"`
	ServiceIdentity      string            `json:"service_identity"`
	NodeID               string            `json:"node_id"`
	Endpoints            []Endpoint        `json:"endpoints"`
	CapabilitiesProvided []CapabilityRef   `json:"capabilities_provided"`
	Labels               map[string]string `json:"labels"`
	Status               string            `json:"status"`
	LastHeartbeat        time.Time         `json:"last_heartbeat"`
}

type Provider struct {
	ServiceID       string            `json:"service_id"`
	ServiceIdentity string            `json:"service_identity"`
	NodeID          string            `json:"node_id"`
	CapabilityID    string            `json:"capability_id"`
	TrustTier       TrustTier         `json:"trust_tier"`
	Endpoint        Endpoint          `json:"endpoint"`
	Labels          map[string]string `json:"labels"`
	Distance        ProviderDistance  `json:"distance"`
	Available       bool              `json:"available"`
}

type ProviderDistance string

const (
	DistanceSameNode ProviderDistance = "same_node"
	DistanceSameLAN  ProviderDistance = "same_lan"
	DistanceSameSite ProviderDistance = "same_site"
	DistanceWAN      ProviderDistance = "wan"
	DistanceUnknown  ProviderDistance = "unknown"
)

type Capability struct {
	ID          string                 `json:"id"`
	Version     string                 `json:"version"`
	RiskClass   RiskClass              `json:"risk_class"`
	Description string                 `json:"description"`
	Schema      map[string]interface{} `json:"schema"`
	Digest      string                 `json:"digest"`
	CachedAt    time.Time              `json:"cached_at"`
}

type CapabilityCache struct {
	mu                sync.RWMutex           `json:\"-\"`
	Version           string                 `json:\"version\"`
	Capabilities      map[string]*Capability `json:\"capabilities\"`
	SchemaIndexDigest string                 `json:\"schema_index_digest\"`
	VerifiedAt        time.Time              `json:\"verified_at\"`
}

// GetCapability returns a capability by ID (thread-safe)
func (cc *CapabilityCache) GetCapability(id string) (*Capability, bool) {
	if cc == nil {
		return nil, false
	}
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	cap, exists := cc.Capabilities[id]
	return cap, exists
}

// SetCapability sets a capability (thread-safe)
func (cc *CapabilityCache) SetCapability(id string, cap *Capability) {
	if cc == nil {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.Capabilities == nil {
		cc.Capabilities = make(map[string]*Capability)
	}
	cc.Capabilities[id] = cap
}

// ListCapabilities returns all capability IDs (thread-safe)
func (cc *CapabilityCache) ListCapabilities() []string {
	if cc == nil {
		return nil
	}
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	ids := make([]string, 0, len(cc.Capabilities))
	for id := range cc.Capabilities {
		ids = append(ids, id)
	}
	return ids
}

// List returns all capabilities (thread-safe)
func (cc *CapabilityCache) List() []*Capability {
	if cc == nil {
		return nil
	}
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	result := make([]*Capability, 0, len(cc.Capabilities))
	for _, cap := range cc.Capabilities {
		result = append(result, cap)
	}
	return result
}

type MeshPolicy struct {
	Version     string                `json:"version"`
	Rules       map[string]PolicyRule `json:"rules"`
	Defaults    PolicyDefaults        `json:"defaults"`
	EvaluatedAt time.Time             `json:"evaluated_at"`
}

type PolicyRule struct {
	ID          string                 `json:"id"`
	Condition   string                 `json:"condition"`
	Decision    BindingDecision        `json:"decision"`
	Constraints map[string]interface{} `json:"constraints"`
	Priority    int                    `json:"priority"`
}

type PolicyDefaults struct {
	LowRisk      BindingDecision `json:"low_risk"`
	MediumRisk   BindingDecision `json:"medium_risk"`
	HighRisk     BindingDecision `json:"high_risk"`
	MinTrustTier TrustTier       `json:"min_trust_tier"`
}

type ConsentState struct {
	SubjectRef   string                 `json:"subject_ref"`
	CapabilityID string                 `json:"capability_id"`
	Decision     BindingDecision        `json:"decision"`
	Constraints  map[string]interface{} `json:"constraints"`
	Version      string                 `json:"version"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// ===== Telemetry Types =====

type MeshBindingGrantedEvent struct {
	BindingID           string             `json:"binding_id"`
	ClientServiceID     string             `json:"client_service_id"`
	ProviderServiceID   string             `json:"provider_service_id"`
	CapabilityID        string             `json:"capability_id"`
	ExpiresAt           time.Time          `json:"expires_at"`
	EnforcedConstraints BindingConstraints `json:"enforced_constraints"`
	GrantedAt           time.Time          `json:"granted_at"`
}

type MeshBindingDeniedEvent struct {
	RequestID       string     `json:"request_id"`
	ClientServiceID string     `json:"client_service_id"`
	CapabilityID    string     `json:"capability_id"`
	ReasonCode      ReasonCode `json:"reason_code"`
	PolicyVersion   string     `json:"policy_version,omitempty"`
	CacheVersion    string     `json:"cache_version,omitempty"`
	DeniedAt        time.Time  `json:"denied_at"`
}

type MeshBindingRevokedEvent struct {
	BindingID string    `json:"binding_id"`
	Reason    string    `json:"reason"`
	RevokedAt time.Time `json:"revoked_at"`
}

type MeshPolicyDecisionEvent struct {
	DecisionID   string                 `json:"decision_id"`
	Client       ClientRef              `json:"client"`
	Provider     *ProviderInfo          `json:"provider,omitempty"`
	CapabilityID string                 `json:"capability_id"`
	Decision     BindingDecision        `json:"decision"`
	ReasonCode   ReasonCode             `json:"reason_code"`
	Trace        map[string]interface{} `json:"trace,omitempty"`
}

type MeshHealthChangedEvent struct {
	NewState   MeshState `json:"new_state"`
	Reason     string    `json:"reason"`
	ObservedAt time.Time `json:"observed_at"`
}

type MeshTelemetrySummary struct {
	Interval        string             `json:"interval"`
	Counts          TelemetryCounters  `json:"counts"`
	ErrorRates      map[string]float64 `json:"error_rates"`
	TopDenies       []DenyEntry        `json:"top_denies,omitempty"`
	TopCapabilities []CapabilityEntry  `json:"top_capabilities,omitempty"`
	BufferStats     BufferStatistics   `json:"buffer_stats"`
	CollectedAt     time.Time          `json:"collected_at"`
}

type TelemetryCounters struct {
	BindingsGranted  int64 `json:"bindings_granted"`
	BindingsDenied   int64 `json:"bindings_denied"`
	BindingsRevoked  int64 `json:"bindings_revoked"`
	ConnectionsOpen  int64 `json:"connections_open"`
	BytesTransferred int64 `json:"bytes_transferred"`
}

type DenyEntry struct {
	ReasonCode ReasonCode `json:"reason_code"`
	Count      int64      `json:"count"`
}

type CapabilityEntry struct {
	CapabilityID string `json:"capability_id"`
	RequestCount int64  `json:"request_count"`
}

type BufferStatistics struct {
	TelemetryBytes    int64 `json:"telemetry_bytes"`
	AuditEventsQueued int64 `json:"audit_events_queued"`
}

// ===== Error Types =====

type MMAErrorResponse struct {
	Error struct {
		Code    string        `json:"code"`
		Message string        `json:"message"`
		Details []ErrorDetail `json:"details,omitempty"`
	} `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

type ErrorDetail struct {
	Field        string `json:"field,omitempty"`
	Issue        string `json:"issue"`
	SuggestedFix string `json:"suggested_fix,omitempty"`
}

// ===== Configuration Types =====

type TelemetryFlushRequest struct {
	Types    []string   `json:"types"`
	Since    *time.Time `json:"since,omitempty"`
	MaxBytes int        `json:"max_bytes,omitempty"`
}

type TelemetryFlushResponse struct {
	Delivered       TelemetryCounters `json:"delivered"`
	RemainingQueued int64             `json:"remaining_queued"`
	DeliveryRef     string            `json:"delivery_ref,omitempty"`
	Events          []interface{}     `json:"events,omitempty"`
}
