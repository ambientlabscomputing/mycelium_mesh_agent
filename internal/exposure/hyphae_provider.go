package exposure

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ambientlabscomputing/hyphae/sdk"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
)

// HyphaeProvider implements the Provider interface using Hyphae's tunnel protocol.
// Each active exposure gets its own dedicated TunnelClient (and thus its own
// mTLS connection + yamux session). This ensures that binding a second exposure
// never kills the first one's session.
type HyphaeProvider struct {
	cfg    config.HyphaeConfig
	logger *slog.Logger

	// tunnelCfg is the base SDK config used to create per-exposure clients.
	// Built once in the constructor; shared read-only across all Bind() calls.
	tunnelCfg sdk.TunnelClientConfig

	mu            sync.RWMutex
	activeTunnels map[string]*activeTunnel // exposureID -> tunnel state
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

// NewHyphaeProvider creates a new Hyphae exposure provider with file-based certs.
func NewHyphaeProvider(cfg config.HyphaeConfig, logger *slog.Logger) (*HyphaeProvider, error) {
	return NewHyphaeProviderWithTLS(cfg, nil, logger)
}

// NewHyphaeProviderWithTLS creates a new Hyphae exposure provider with optional pre-built TLS config.
// If tlsCfg is provided, it will be used instead of loading cert/key files from disk.
// This is useful when certificates are bootstrapped dynamically from the kernel.
func NewHyphaeProviderWithTLS(cfg config.HyphaeConfig, tlsCfg *tls.Config, logger *slog.Logger) (*HyphaeProvider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("provider", "hyphae")

	// Build the base config used for each per-exposure TunnelClient.
	// We do NOT create a client here — each Bind() creates its own dedicated one.
	tunnelCfg := sdk.TunnelClientConfig{
		HyphaeAddr:     cfg.TunnelAddr,
		CACertPath:     cfg.CACertPath,
		ClientCertPath: cfg.ClientCertPath,
		ClientKeyPath:  cfg.ClientKeyPath,
		AutoReconnect:  cfg.AutoReconnect,
		TLSConfig:      tlsCfg, // may be nil; SDK loads certs from paths
	}

	return &HyphaeProvider{
		cfg:           cfg,
		logger:        logger,
		tunnelCfg:     tunnelCfg,
		activeTunnels: make(map[string]*activeTunnel),
	}, nil
}

// Name returns the provider name.
func (p *HyphaeProvider) Name() string {
	return "hyphae"
}

// Bind establishes a tunnel for the given exposure.
// A fresh TunnelClient is created per exposure so that binding a new exposure
// never disrupts an existing one's yamux session.
func (p *HyphaeProvider) Bind(ctx context.Context, req *BindRequest) (*BindResult, error) {
	logger := p.logger.With(
		"exposure_id", req.ExposureID,
		"lease_id", req.LeaseID,
		"hostname", req.Hostname,
		"target_port", req.TargetPort,
		"local_addr", req.LocalAddr,
	)

	// Create a dedicated TunnelClient for this exposure.
	client, err := sdk.NewTunnelClient(p.tunnelCfg)
	if err != nil {
		logger.Error("failed to create tunnel client", "error", err)
		return nil, fmt.Errorf("failed to create tunnel client: %w", err)
	}

	// Connect the per-exposure client with the given lease ID.
	if err := client.Connect(ctx, req.LeaseID); err != nil {
		client.Close() //nolint:errcheck
		logger.Error("failed to connect tunnel", "error", err)
		return nil, fmt.Errorf("failed to connect tunnel: %w", err)
	}

	// Create a context for this specific tunnel, rooted at Background so it
	// is NOT tied to the event-handler context (which is cancelled as soon as
	// the handler function returns). The Forward goroutine must outlive the
	// handler. The cancel function is stored in activeTunnels and called by
	// Unbind() when the exposure is torn down.
	tunnelCtx, cancel := context.WithCancel(context.Background())

	// Start forwarding in a goroutine.
	forwardDone := make(chan error, 1)
	go func() {
		forwardDone <- client.Forward(tunnelCtx, req.LocalAddr)
	}()

	// Store the active tunnel with its dedicated client.
	p.mu.Lock()
	p.activeTunnels[req.ExposureID] = &activeTunnel{
		exposureID:  req.ExposureID,
		leaseID:     req.LeaseID,
		hostname:    req.Hostname,
		localAddr:   req.LocalAddr,
		status:      "bound",
		client:      client,
		ctx:         tunnelCtx,
		cancel:      cancel,
		forwardDone: forwardDone,
	}
	p.mu.Unlock()

	logger.Info("exposure tunnel bound successfully",
		"provider", "hyphae",
		"lease_id", req.LeaseID,
		"hostname", req.Hostname,
		"target_port", req.TargetPort,
		"local_addr", req.LocalAddr,
	)

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

	// Cancel the forwarding context and close the per-exposure client.
	// Closing the client shuts down the yamux session + supervisor goroutine.
	tunnel.cancel()
	if err := tunnel.client.Close(); err != nil {
		logger.Warn("error closing tunnel client", "error", err)
	}

	// Wait for Forward() to exit with a timeout.
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

	// Cancel context and close the dedicated client for each active tunnel.
	for _, tunnel := range tunnels {
		tunnel.cancel()
		if err := tunnel.client.Close(); err != nil {
			logger.Error("failed to close tunnel client",
				"exposure_id", tunnel.exposureID, "error", err)
		}
	}

	logger.Info("hyphae provider closed")
	return nil
}
