package host_adapter

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Adapter interface for bridging devices into the mesh.
type Adapter interface {
	// Start initializes the adapter.
	Start(ctx context.Context) error

	// Stop cleanly shuts down the adapter.
	Stop(ctx context.Context) error

	// RegisterService registers a device service.
	RegisterService(service *types.ServiceEntry) error

	// UnregisterService removes a device service.
	UnregisterService(serviceID string) error

	// AdvertiseCapability advertises a capability from a device.
	AdvertiseCapability(serviceID string, cap *types.Capability) error

	// RevokeCapability removes a capability advertisement.
	RevokeCapability(serviceID string, capabilityID string) error

	// GetService returns service details.
	GetService(serviceID string) *types.ServiceEntry

	// ListServices returns all registered services.
	ListServices() []*types.ServiceEntry

	// HandleRequest processes a binding request to a device service.
	HandleRequest(ctx context.Context, serviceID string, req *types.BindingRequest) (*types.BindingResponse, error)
}

// DeviceProvider implements the Adapter interface for device management.
type DeviceProvider struct {
	mu       sync.RWMutex
	log      *slog.Logger
	services map[string]*types.ServiceEntry // serviceID -> ServiceEntry
}

// NewDeviceProvider creates a new device provider adapter.
func NewDeviceProvider(log *slog.Logger) *DeviceProvider {
	return &DeviceProvider{
		log:      log,
		services: make(map[string]*types.ServiceEntry),
	}
}

// Start initializes the device provider.
func (dp *DeviceProvider) Start(ctx context.Context) error {
	dp.log.Info("Device provider started")
	return nil
}

// Stop shuts down the device provider.
func (dp *DeviceProvider) Stop(ctx context.Context) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.log.Info("Device provider stopped", "service_count", len(dp.services))
	dp.services = make(map[string]*types.ServiceEntry)
	return nil
}

// RegisterService registers a device service.
func (dp *DeviceProvider) RegisterService(service *types.ServiceEntry) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if service == nil || service.ServiceID == "" {
		return types.ErrInvalidConfig
	}

	dp.services[service.ServiceID] = service
	dp.log.Info("Service registered", "service_id", service.ServiceID)
	return nil
}

// UnregisterService removes a device service.
func (dp *DeviceProvider) UnregisterService(serviceID string) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if _, exists := dp.services[serviceID]; !exists {
		return types.ErrCapabilityNotFound
	}

	delete(dp.services, serviceID)
	dp.log.Info("Service unregistered", "service_id", serviceID)
	return nil
}

// AdvertiseCapability advertises a capability from a device service.
func (dp *DeviceProvider) AdvertiseCapability(serviceID string, cap *types.Capability) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	service, exists := dp.services[serviceID]
	if !exists {
		return types.ErrCapabilityNotFound
	}

	if cap == nil || cap.ID == "" {
		return types.ErrInvalidConfig
	}

	// Store capability in the service capabilities list
	service.CapabilitiesProvided = append(service.CapabilitiesProvided, types.CapabilityRef{
		CapabilityID: cap.ID,
		Version:      cap.Version,
	})

	dp.log.Info("Capability advertised", "service_id", serviceID, "capability_id", cap.ID)
	return nil
}

// RevokeCapability removes a capability advertisement.
func (dp *DeviceProvider) RevokeCapability(serviceID string, capabilityID string) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	service, exists := dp.services[serviceID]
	if !exists {
		return types.ErrCapabilityNotFound
	}

	// Remove capability from the service capabilities list
	filtered := make([]types.CapabilityRef, 0, len(service.CapabilitiesProvided))
	for _, cap := range service.CapabilitiesProvided {
		if cap.CapabilityID != capabilityID {
			filtered = append(filtered, cap)
		}
	}
	service.CapabilitiesProvided = filtered

	dp.log.Info("Capability revoked", "service_id", serviceID, "capability_id", capabilityID)
	return nil
}

// GetService returns service details.
func (dp *DeviceProvider) GetService(serviceID string) *types.ServiceEntry {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	return dp.services[serviceID]
}

// ListServices returns all registered services.
func (dp *DeviceProvider) ListServices() []*types.ServiceEntry {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	result := make([]*types.ServiceEntry, 0, len(dp.services))
	for _, s := range dp.services {
		result = append(result, s)
	}
	return result
}

// HandleRequest processes a binding request to a device service.
func (dp *DeviceProvider) HandleRequest(ctx context.Context, serviceID string, req *types.BindingRequest) (*types.BindingResponse, error) {
	service := dp.GetService(serviceID)
	if service == nil {
		return nil, types.ErrCapabilityNotFound
	}

	// Verify capability is advertised
	found := false
	for _, cap := range service.CapabilitiesProvided {
		if cap.CapabilityID == req.Capability.ID {
			found = true
			break
		}
	}
	if !found {
		return nil, types.ErrCapabilityNotFound
	}

	// Return a stub response
	resp := &types.BindingResponse{
		Decision:   types.BindingAllow,
		ReasonCode: "",
		BindingID:  req.RequestID,
		Provider: &types.ProviderInfo{
			ServiceID: service.ServiceID,
			Identity:  service.ServiceIdentity,
		},
	}

	if len(service.Endpoints) > 0 {
		resp.Provider.Endpoint = service.Endpoints[0]
	}

	return resp, nil
}

// Manager manages multiple device adapters.
type Manager struct {
	mu       sync.RWMutex
	log      *slog.Logger
	adapters map[string]Adapter // name -> Adapter
}

// NewManager creates a new adapter manager.
func NewManager(log *slog.Logger) *Manager {
	return &Manager{
		log:      log,
		adapters: make(map[string]Adapter),
	}
}

// RegisterAdapter registers an adapter by name.
func (m *Manager) RegisterAdapter(name string, adapter Adapter) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if adapter == nil {
		return types.ErrInvalidConfig
	}

	m.adapters[name] = adapter
	m.log.Info("Adapter registered", "name", name)
	return nil
}

// UnregisterAdapter removes an adapter.
func (m *Manager) UnregisterAdapter(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.adapters[name]; !exists {
		return types.ErrCapabilityNotFound
	}

	delete(m.adapters, name)
	m.log.Info("Adapter unregistered", "name", name)
	return nil
}

// GetAdapter returns an adapter by name.
func (m *Manager) GetAdapter(name string) Adapter {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.adapters[name]
}

// ListAdapters returns all registered adapters.
func (m *Manager) ListAdapters() map[string]Adapter {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]Adapter)
	for k, v := range m.adapters {
		result[k] = v
	}
	return result
}

// StartAll starts all registered adapters.
func (m *Manager) StartAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, adapter := range m.adapters {
		if err := adapter.Start(ctx); err != nil {
			m.log.Error("Failed to start adapter", "name", name, "error", err)
			return err
		}
	}
	return nil
}

// StopAll stops all registered adapters.
func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, adapter := range m.adapters {
		if err := adapter.Stop(ctx); err != nil {
			m.log.Error("Failed to stop adapter", "name", name, "error", err)
		}
	}
	return nil
}
