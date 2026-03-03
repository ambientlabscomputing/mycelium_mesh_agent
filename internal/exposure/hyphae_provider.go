package exposure

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ambientlabscomputing/hyphae/sdk"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
)

// HyphaeProvider implements the Provider interface using Hyphae's tunnel protocol.
type HyphaeProvider struct {
	cfg    config.HyphaeConfig
	logger *slog.Logger

	mu            sync.RWMutex
	activeTunnels map[string]*activeTunnel // exposureID -> tunnel state

	// Client that will be reused across multiple exposures
	tunnelClient *sdk.TunnelClient
}

// activeTunnel represents a single active tunnel connection.
type activeTunnel struct {
	exposureID  string
	leaseID     string
	hostname    string
	localAddr   string
	status      string
	client      *sdk.TunnelClient
	ctx         context.Context
	cancel      context.CancelFunc
	forwardDone chan error // closed when Forward() exits
}

// NewHyphaeProvider creates a new Hyphae exposure provider.
func NewHyphaeProvider(cfg config.HyphaeConfig, logger *slog.Logger) (*HyphaeProvider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("provider", "hyphae")

	// Create the tunnel client that will be reused
	tunnelCfg := sdk.TunnelClientConfig{
		HyphaeAddr:     cfg.TunnelAddr,
		CACertPath:     cfg.CACertPath,
		ClientCertPath: cfg.ClientCertPath,
		ClientKeyPath:  cfg.ClientKeyPath,
		AutoReconnect:  cfg.AutoReconnect,
	}

	tunnelClient, err := sdk.NewTunnelClient(tunnelCfg)
	if err != nil {
		logger.Error("failed to create tunnel client", "error", err)
		return nil, fmt.Errorf("failed to create tunnel client: %w", err)
	}

	return &HyphaeProvider{
		cfg:           cfg,
		logger:        logger,
		activeTunnels: make(map[string]*activeTunnel),
		tunnelClient:  tunnelClient,
	}, nil
}

// Name returns the provider name.
func (p *HyphaeProvider) Name() string {
	return "hyphae"
}

// Bind establishes a tunnel for the given exposure.
// This method blocks until the tunnel is established or an error occurs.
func (p *HyphaeProvider) Bind(ctx context.Context, req *BindRequest) (*BindResult, error) {
	logger := p.logger.With(
		"exposure_id", req.ExposureID,
		"lease_id", req.LeaseID,
		"hostname", req.Hostname,
		"target_port", req.TargetPort,
		"local_addr", req.LocalAddr,
	)

	// Connect the tunnel client with the given lease ID
	if err := p.tunnelClient.Connect(ctx, req.LeaseID); err != nil {
		logger.Error("failed to connect tunnel", "error", err)
		return nil, fmt.Errorf("failed to connect tunnel: %w", err)
	}

	// Create a context for this specific tunnel
	tunnelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start forwarding in a goroutine
	forwardDone := make(chan error, 1)
	go func() {
		forwardDone <- p.tunnelClient.Forward(tunnelCtx, req.LocalAddr)
	}()

	// Store the active tunnel
	p.mu.Lock()
	p.activeTunnels[req.ExposureID] = &activeTunnel{
		exposureID:  req.ExposureID,
		leaseID:     req.LeaseID,
		hostname:    req.Hostname,
		localAddr:   req.LocalAddr,
		status:      "bound",
		client:      p.tunnelClient,
		ctx:         tunnelCtx,
		cancel:      cancel,
		forwardDone: forwardDone,
	}
	p.mu.Unlock()

	logger.Info("exposure tunnel bound successfully")

	// Build the public URL
	publicURL := fmt.Sprintf("https://%s", req.Hostname)

	return &BindResult{
		ExposureID: req.ExposureID,
		LeaseID:    req.LeaseID,
		PublicURL:  publicURL,
		Status:     "bound",
	}, nil
}

// Unbind tears down the tunnel for the given exposure.
func (p *HyphaeProvider) Unbind(ctx context.Context, exposureID string) error {
	logger := p.logger.With("exposure_id", exposureID)

	p.mu.Lock()
	tunnel, exists := p.activeTunnels[exposureID]
	if !exists {
		p.mu.Unlock()
		logger.Warn("tunnel not found, may already be unbound")
		return nil
	}
	delete(p.activeTunnels, exposureID)
	p.mu.Unlock()

	// Cancel the forwarding context
	tunnel.cancel()

	// Wait for forwarding to finish with a timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case <-tunnel.forwardDone:
		logger.Info("exposure tunnel unbound successfully")
		return nil
	case <-ctx.Done():
		logger.Warn("timeout waiting for forward to exit")
		return fmt.Errorf("timeout unbinding exposure")
	}
}

// Status returns the current status of an active exposure.
func (p *HyphaeProvider) Status(exposureID string) (string, error) {
	p.mu.RLock()
	tunnel, exists := p.activeTunnels[exposureID]
	p.mu.RUnlock()

	if !exists {
		return "unknown", fmt.Errorf("exposure not found: %s", exposureID)
	}

	return tunnel.status, nil
}

// Close shuts down the provider and all active tunnels.
func (p *HyphaeProvider) Close() error {
	logger := p.logger

	p.mu.Lock()
	tunnels := make([]*activeTunnel, 0, len(p.activeTunnels))
	for _, tunnel := range p.activeTunnels {
		tunnels = append(tunnels, tunnel)
	}
	p.mu.Unlock()

	// Cancel all tunnels
	for _, tunnel := range tunnels {
		tunnel.cancel()
	}

	// Close the underlying tunnel client
	if p.tunnelClient != nil {
		if err := p.tunnelClient.Close(); err != nil {
			logger.Error("failed to close tunnel client", "error", err)
			return err
		}
	}

	logger.Info("hyphae provider closed")
	return nil
}
