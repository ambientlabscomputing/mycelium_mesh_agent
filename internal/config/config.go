package config

import (
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// MMAConfig is the main configuration for the Mycelium Mesh Agent.
// Loaded from UA via /v1/config/apply endpoint and stored in thread-safe store.
type MMAConfig struct {
	// ConfigVersion is the version of this configuration.
	// Must be provided by UA and is used for validation.
	ConfigVersion string `yaml:"config_version" json:"config_version"`

	// MeshMode specifies the operation mode: "sidecar", "ambient", or "sdk"
	MeshMode string `yaml:"mesh_mode" json:"mesh_mode"`

	// LocalityDefaults specifies default locality preference.
	// Options: "node_only", "lan_preferred", "lan_only", "wan_allowed"
	LocalityDefaults string `yaml:"locality_defaults" json:"locality_defaults"`

	// TelemetryDefaults specifies telemetry behavior.
	TelemetryDefaults TelemetryConfig `yaml:"telemetry_defaults" json:"telemetry_defaults"`

	// ControlChannelConfig specifies how to communicate with UA.
	ControlChannelConfig ControlChannelConfig `yaml:"control_channel_config" json:"control_channel_config"`

	// PolicyRef points to UA-stored policy (can be a URI, file path, or reference)
	PolicyRef string `yaml:"policy_ref" json:"policy_ref"`

	// CapabilityCacheRef points to UA-stored capability cache
	CapabilityCacheRef string `yaml:"capability_cache_ref" json:"capability_cache_ref"`

	// DiscoveryConfig specifies discovery behavior
	DiscoveryConfig DiscoveryConfig `yaml:"discovery_config" json:"discovery_config"`

	// BindingEngineConfig specifies binding engine behavior
	BindingEngineConfig BindingEngineConfig `yaml:"binding_engine_config" json:"binding_engine_config"`

	// RuntimeConfig specifies mesh runtime behavior
	RuntimeConfig RuntimeConfig `yaml:"runtime_config" json:"runtime_config"`

	// AppliedAt records when this config was applied
	AppliedAt time.Time `json:"applied_at"`
}

// TelemetryConfig specifies telemetry behavior
type TelemetryConfig struct {
	// Level: "debug", "info", "warn", "error"
	Level string `yaml:"level" json:"level"`

	// BufferSizeBytes is the max telemetry buffer size
	BufferSizeBytes int `yaml:"buffer_size_bytes" json:"buffer_size_bytes"`

	// FlushIntervalMs is how often to flush telemetry to UA
	FlushIntervalMs int `yaml:"flush_interval_ms" json:"flush_interval_ms"`

	// IncludeTraces whether to include detailed traces in telemetry
	IncludeTraces bool `yaml:"include_traces" json:"include_traces"`
}

// ControlChannelConfig specifies how MMA communicates with UA
type ControlChannelConfig struct {
	// Transport: "grpc_uds" or "grpc_tcp"
	Transport string `yaml:"transport" json:"transport"`

	// Address: "/tmp/ua.sock" for UDS or "localhost:8000" for TCP
	Address string `yaml:"address" json:"address"`

	// TLSEnabled whether to use TLS for communication
	TLSEnabled bool `yaml:"tls_enabled" json:"tls_enabled"`

	// TLSCertRef path to client certificate
	TLSCertRef string `yaml:"tls_cert_ref" json:"tls_cert_ref"`

	// TLSKeyRef path to client private key
	TLSKeyRef string `yaml:"tls_key_ref" json:"tls_key_ref"`

	// TLSCARef path to CA certificate for verifying UA
	TLSCARef string `yaml:"tls_ca_ref" json:"tls_ca_ref"`

	// ReconnectIntervalMs is how often to try reconnecting if down
	ReconnectIntervalMs int `yaml:"reconnect_interval_ms" json:"reconnect_interval_ms"`
}

// DiscoveryConfig specifies discovery behavior
type DiscoveryConfig struct {
	// UAEventStreamEnabled whether to consume UA event stream
	UAEventStreamEnabled bool `yaml:"ua_event_stream_enabled" json:"ua_event_stream_enabled"`

	// MDNSEnabled whether to do optional LAN mDNS discovery (hints only)
	MDNSEnabled bool `yaml:"mdns_enabled" json:"mdns_enabled"`

	// MDNSServiceName the service name to advertise via mDNS (e.g., "_mycelium._tcp")
	MDNSServiceName string `yaml:"mdns_service_name" json:"mdns_service_name"`

	// CacheCleanupIntervalMs how often to clean up stale entries
	CacheCleanupIntervalMs int `yaml:"cache_cleanup_interval_ms" json:"cache_cleanup_interval_ms"`
}

// BindingEngineConfig specifies binding engine behavior
type BindingEngineConfig struct {
	// DefaultTTLMs default TTL for bindings if not specified
	DefaultTTLMs int `yaml:"default_ttl_ms" json:"default_ttl_ms"`

	// MaxTTLMs maximum allowed TTL for bindings
	MaxTTLMs int `yaml:"max_ttl_ms" json:"max_ttl_ms"`

	// DefaultLocalityPreference default locality if not specified
	DefaultLocalityPreference string `yaml:"default_locality_preference" json:"default_locality_preference"`

	// RequireExplicitConsent whether to require explicit consent for high-risk caps
	RequireExplicitConsent bool `yaml:"require_explicit_consent" json:"require_explicit_consent"`

	// RateLimitDefaults specifies default rate limiting
	RateLimitDefaults RateLimitDefaults `yaml:"rate_limit_defaults" json:"rate_limit_defaults"`
}

// RateLimitDefaults specifies default rate limiting
type RateLimitDefaults struct {
	// DefaultRPS default requests per second (0 = unlimited)
	DefaultRPS float64 `yaml:"default_rps" json:"default_rps"`

	// DefaultBurst default burst size (0 = no burst)
	DefaultBurst int `yaml:"default_burst" json:"default_burst"`
}

// RuntimeConfig specifies mesh runtime behavior
type RuntimeConfig struct {
	// DataPlaneMode: "ambient", "sidecar", "sdk"
	DataPlaneMode string `yaml:"data_plane_mode" json:"data_plane_mode"`

	// ConnectionPoolSize max connections to pool
	ConnectionPoolSize int `yaml:"connection_pool_size" json:"connection_pool_size"`

	// ConnectionTimeoutMs timeout for establishing connections
	ConnectionTimeoutMs int `yaml:"connection_timeout_ms" json:"connection_timeout_ms"`

	// ConnectionIdleTimeoutMs idle timeout before closing connection
	ConnectionIdleTimeoutMs int `yaml:"connection_idle_timeout_ms" json:"connection_idle_timeout_ms"`

	// MaxConcurrentBindings max concurrent binding operations
	MaxConcurrentBindings int `yaml:"max_concurrent_bindings" json:"max_concurrent_bindings"`

	// EnableQUIC whether to enable QUIC for WAN connections
	EnableQUIC bool `yaml:"enable_quic" json:"enable_quic"`

	// MaxBindingDecisionLatencyMs performance target
	MaxBindingDecisionLatencyMs int `yaml:"max_binding_decision_latency_ms" json:"max_binding_decision_latency_ms"`
}

// DefaultMMAConfig returns a configuration with sensible defaults
func DefaultMMAConfig() *MMAConfig {
	return &MMAConfig{
		ConfigVersion:    "1.0.0",
		MeshMode:         "ambient",
		LocalityDefaults: string(types.LocalityLANPreferred),
		AppliedAt:        time.Now(),
		TelemetryDefaults: TelemetryConfig{
			Level:           "info",
			BufferSizeBytes: 1024 * 1024, // 1MB
			FlushIntervalMs: 30000,       // 30 seconds
			IncludeTraces:   false,
		},
		ControlChannelConfig: ControlChannelConfig{
			Transport:           "grpc_uds",
			Address:             "/tmp/ua_mma.sock",
			TLSEnabled:          false,
			ReconnectIntervalMs: 5000,
		},
		DiscoveryConfig: DiscoveryConfig{
			UAEventStreamEnabled:   true,
			MDNSEnabled:            true,
			MDNSServiceName:        "_mycelium._tcp",
			CacheCleanupIntervalMs: 60000,
		},
		BindingEngineConfig: BindingEngineConfig{
			DefaultTTLMs:              3600000,  // 1 hour
			MaxTTLMs:                  86400000, // 24 hours
			DefaultLocalityPreference: string(types.LocalityLANPreferred),
			RequireExplicitConsent:    false,
			RateLimitDefaults: RateLimitDefaults{
				DefaultRPS:   0, // unlimited
				DefaultBurst: 10,
			},
		},
		RuntimeConfig: RuntimeConfig{
			DataPlaneMode:               "ambient",
			ConnectionPoolSize:          100,
			ConnectionTimeoutMs:         5000,
			ConnectionIdleTimeoutMs:     30000,
			MaxConcurrentBindings:       1000,
			EnableQUIC:                  true,
			MaxBindingDecisionLatencyMs: 10,
		},
	}
}
