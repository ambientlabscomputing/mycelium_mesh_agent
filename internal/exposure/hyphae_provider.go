package exposure

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
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
	stopProxy   func()     // non-nil if a URL reverse proxy is running for this tunnel
}

// NewHyphaeProvider creates a new Hyphae exposure provider with file-based certs.
func NewHyphaeProvider(cfg config.HyphaeConfig, logger *slog.Logger) (*HyphaeProvider, error) {
	return NewHyphaeProviderWithTLS(cfg, nil, nil, logger)
}

// NewHyphaeProviderWithTLS creates a new Hyphae exposure provider with optional pre-built TLS config.
// If tlsCfg is provided, it will be used instead of loading cert/key files from disk.
// If refresher is provided, it is called on x509 verification errors to fetch a fresh CA pool and retry.
// This is useful when certificates are bootstrapped dynamically from the kernel.
func NewHyphaeProviderWithTLS(cfg config.HyphaeConfig, tlsCfg *tls.Config, refresher func(context.Context) (*x509.CertPool, error), logger *slog.Logger) (*HyphaeProvider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("provider", "hyphae")

	// Build the base config used for each per-exposure TunnelClient.
	// We do NOT create a client here — each Bind() creates its own dedicated one.
	tunnelCfg := sdk.TunnelClientConfig{
		HyphaeAddr:      cfg.TunnelAddr,
		CACertPath:      cfg.CACertPath,
		ClientCertPath:  cfg.ClientCertPath,
		ClientKeyPath:   cfg.ClientKeyPath,
		AutoReconnect:   cfg.AutoReconnect,
		TLSConfig:       tlsCfg,    // may be nil; SDK loads certs from paths
		CACertRefresher: refresher, // may be nil; enables self-healing on cert rotation
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
//
// Bind is idempotent: if a tunnel already exists for the given exposure ID, it
// is torn down first to avoid leaking supervisors and creating a reconnect storm.
func (p *HyphaeProvider) Bind(ctx context.Context, req *BindRequest) (*BindResult, error) {
	logger := p.logger.With(
		"exposure_id", req.ExposureID,
		"lease_id", req.LeaseID,
		"hostname", req.Hostname,
		"target_port", req.TargetPort,
		"local_addr", req.LocalAddr,
	)

	// Tear down any existing tunnel for this exposure before creating a new one.
	// Without this, duplicate bind events (e.g. server_api double-publish or
	// gRPC stream replay) leak TunnelClients whose supervisors compete for the
	// same lease, creating an infinite reconnect storm on the hyphae server.
	p.mu.RLock()
	existing, alreadyBound := p.activeTunnels[req.ExposureID]
	p.mu.RUnlock()
	if alreadyBound {
		logger.Warn("exposure already bound, tearing down stale tunnel before rebind",
			"old_lease_id", existing.leaseID,
			"old_hostname", existing.hostname,
		)
		existing.cancel()
		if err := existing.client.Close(); err != nil {
			logger.Warn("error closing stale tunnel client during rebind", "error", err)
		}
		p.mu.Lock()
		delete(p.activeTunnels, req.ExposureID)
		p.mu.Unlock()
	}

	// Build the per-exposure SDK config, allowing server_api to override the tunnel
	// address on a per-exposure basis via the hyphae_tunnel_addr event field.
	exposureCfg := p.tunnelCfg
	if req.TunnelAddr != "" && req.TunnelAddr != exposureCfg.HyphaeAddr {
		exposureCfg.HyphaeAddr = req.TunnelAddr
		logger.Info("using per-exposure Hyphae tunnel addr",
			"addr", req.TunnelAddr,
			"default_addr", p.tunnelCfg.HyphaeAddr,
		)
	}

	// If a target URL is specified, start a local reverse proxy and use its
	// loopback address as the forwarding target for the Hyphae tunnel.
	effectiveLocalAddr := req.LocalAddr
	var stopProxy func()
	if req.TargetURL != "" {
		proxyAddr, proxyStop, err := StartURLProxy(ctx, req.TargetURL)
		if err != nil {
			logger.Error("failed to start URL proxy", "error", err, "target_url", req.TargetURL)
			return nil, fmt.Errorf("failed to start URL proxy: %w", err)
		}
		effectiveLocalAddr = proxyAddr
		stopProxy = proxyStop
		logger.Info("started URL proxy", "proxy_addr", proxyAddr, "target_url", req.TargetURL)
	}

	// Create a dedicated TunnelClient for this exposure.
	client, err := sdk.NewTunnelClient(exposureCfg)
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

	// ── TCP health check ─────────────────────────────────────────────────
	// Verify the local service is actually reachable before we report
	// success. Without this check, the tunnel appears "bound" in Hyphae
	// while all traffic silently fails with connection-refused upstream.
	healthConn, dialErr := net.DialTimeout("tcp", effectiveLocalAddr, 5*time.Second)
	if dialErr != nil {
		client.Close() //nolint:errcheck
		logger.Error("local service is not reachable — aborting exposure bind",
			"local_addr", effectiveLocalAddr,
			"error", dialErr,
		)
		return nil, fmt.Errorf("local service unreachable at %s: %w", effectiveLocalAddr, dialErr)
	}
	healthConn.Close()

	// Create a context for this specific tunnel, rooted at Background so it
	// is NOT tied to the event-handler context (which is cancelled as soon as
	// the handler function returns). The Forward goroutine must outlive the
	// handler. The cancel function is stored in activeTunnels and called by
	// Unbind() when the exposure is torn down.
	tunnelCtx, cancel := context.WithCancel(context.Background())

	// Start forwarding in a goroutine.
	forwardDone := make(chan error, 1)
	go func() {
		forwardDone <- client.Forward(tunnelCtx, effectiveLocalAddr)
	}()

	// Store the active tunnel with its dedicated client.
	p.mu.Lock()
	p.activeTunnels[req.ExposureID] = &activeTunnel{
		exposureID:  req.ExposureID,
		leaseID:     req.LeaseID,
		hostname:    req.Hostname,
		localAddr:   effectiveLocalAddr,
		status:      "success",
		client:      client,
		ctx:         tunnelCtx,
		cancel:      cancel,
		forwardDone: forwardDone,
		stopProxy:   stopProxy,
	}
	p.mu.Unlock()

	logger.Info("exposure tunnel bound successfully",
		"provider", "hyphae",
		"lease_id", req.LeaseID,
		"hostname", req.Hostname,
		"target_port", req.TargetPort,
		"local_addr", effectiveLocalAddr,
	)

	// Build the public URL
	publicURL := fmt.Sprintf("https://%s", req.Hostname)

	return &BindResult{
		ExposureID: req.ExposureID,
		LeaseID:    req.LeaseID,
		PublicURL:  publicURL,
		Status:     "success",
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

	// Stop the URL reverse proxy, if one was started for this tunnel.
	if tunnel.stopProxy != nil {
		tunnel.stopProxy()
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
