package config

import (
	"sync"
	"time"
)

// Store provides thread-safe access to MMA configuration.
// Configuration can be updated by UA via /v1/config/apply endpoint.
type Store struct {
	mu     sync.RWMutex
	config *MMAConfig
}

// NewStore creates a new configuration store with defaults
func NewStore() *Store {
	return &Store{
		config: DefaultMMAConfig(),
	}
}

// Get returns the current configuration (thread-safe read)
func (s *Store) Get() *MMAConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a shallow copy to prevent external mutations
	if s.config == nil {
		return DefaultMMAConfig()
	}

	cfgCopy := *s.config
	return &cfgCopy
}

// Set updates the configuration (thread-safe write)
// Returns the previous configuration for rollback purposes
func (s *Store) Set(newConfig *MMAConfig) *MMAConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldConfig := s.config
	s.config = newConfig
	return oldConfig
}

// Update applies a mutation function to the config and returns success
func (s *Store) Update(fn func(*MMAConfig) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := fn(s.config); err != nil {
		return err
	}
	s.config.AppliedAt = time.Now()
	return nil
}

// GetMeshMode returns the current mesh mode
func (s *Store) GetMeshMode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.MeshMode
}

// GetLocalityDefaults returns the default locality preference
func (s *Store) GetLocalityDefaults() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.LocalityDefaults
}

// GetTelemetryConfig returns the telemetry configuration
func (s *Store) GetTelemetryConfig() TelemetryConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.TelemetryDefaults
}

// GetControlChannelConfig returns the control channel configuration
func (s *Store) GetControlChannelConfig() ControlChannelConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.ControlChannelConfig
}

// GetDiscoveryConfig returns the discovery configuration
func (s *Store) GetDiscoveryConfig() DiscoveryConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.DiscoveryConfig
}

// GetBindingEngineConfig returns the binding engine configuration
func (s *Store) GetBindingEngineConfig() BindingEngineConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.BindingEngineConfig
}

// GetRuntimeConfig returns the mesh runtime configuration
func (s *Store) GetRuntimeConfig() RuntimeConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.RuntimeConfig
}

// GetConfigVersion returns the current config version
func (s *Store) GetConfigVersion() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.ConfigVersion
}

// GetAppliedAt returns when the config was last applied
func (s *Store) GetAppliedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.AppliedAt
}

// ValidateConfig performs validation checks on configuration
func (s *Store) ValidateConfig(cfg *MMAConfig) error {
	// Validate MeshMode
	if cfg.MeshMode != "sidecar" && cfg.MeshMode != "ambient" && cfg.MeshMode != "sdk" {
		return NewConfigError("invalid mesh_mode: must be 'sidecar', 'ambient', or 'sdk'")
	}

	// Validate ControlChannelConfig
	if cfg.ControlChannelConfig.Transport != "grpc_uds" && cfg.ControlChannelConfig.Transport != "grpc_tcp" {
		return NewConfigError("invalid control_channel_config.transport: must be 'grpc_uds' or 'grpc_tcp'")
	}

	if cfg.ControlChannelConfig.Address == "" {
		return NewConfigError("control_channel_config.address cannot be empty")
	}

	// Validate BindingEngineConfig
	if cfg.BindingEngineConfig.DefaultTTLMs <= 0 {
		return NewConfigError("binding_engine_config.default_ttl_ms must be > 0")
	}

	if cfg.BindingEngineConfig.MaxTTLMs < cfg.BindingEngineConfig.DefaultTTLMs {
		return NewConfigError("binding_engine_config.max_ttl_ms must be >= default_ttl_ms")
	}

	// Validate RuntimeConfig
	if cfg.RuntimeConfig.ConnectionPoolSize <= 0 {
		return NewConfigError("runtime_config.connection_pool_size must be > 0")
	}

	if cfg.RuntimeConfig.ConnectionTimeoutMs <= 0 {
		return NewConfigError("runtime_config.connection_timeout_ms must be > 0")
	}

	return nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Message string
}

// NewConfigError creates a new ConfigError
func NewConfigError(message string) *ConfigError {
	return &ConfigError{Message: message}
}

// Error implements the error interface
func (e *ConfigError) Error() string {
	return "config error: " + e.Message
}
