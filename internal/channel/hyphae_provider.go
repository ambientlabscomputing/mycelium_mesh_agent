package channel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/hashicorp/yamux"
)

// HyphaeProvider implements Provider using the Hyphae relay channel protocol.
//
// The channel protocol is a custom HTTP-upgrade handshake over mTLS:
//   - Listener:  X-Listener-Register: true + X-Org-ID  → 101 → yamux.Client
//   - Initiator: X-Channel-ID + X-Channel-Grant         → 101 → raw net.Conn
//
// One goroutine per active listener runs in the background, accepting yamux
// streams from Hyphae and holding them for future data-plane use.  Each
// initiator connection is a one-shot dial and is also stored for lifecycle
// management.
type HyphaeProvider struct {
	cfg    config.HyphaeConfig
	tlsCfg *tls.Config // may be nil; falls back to file-based certs
	logger *slog.Logger

	mu     sync.Mutex
	active map[string]*activeChannel // channelID → state
}

type activeChannel struct {
	cancel  context.CancelFunc
	conn    net.Conn       // nil for listener (uses session instead)
	session *yamux.Session // nil for initiator
}

// NewHyphaeProvider creates a provider using file-based mTLS certs.
func NewHyphaeProvider(cfg config.HyphaeConfig, logger *slog.Logger) *HyphaeProvider {
	return NewHyphaeProviderWithTLS(cfg, nil, logger)
}

// NewHyphaeProviderWithTLS creates a provider with an optional pre-built TLS config.
// Pass tlsCfg=nil to use cert files from cfg.
func NewHyphaeProviderWithTLS(cfg config.HyphaeConfig, tlsCfg *tls.Config, logger *slog.Logger) *HyphaeProvider {
	if logger == nil {
		logger = slog.Default()
	}
	return &HyphaeProvider{
		cfg:    cfg,
		tlsCfg: tlsCfg,
		logger: logger.With("provider", "hyphae_channel"),
		active: make(map[string]*activeChannel),
	}
}

// Name returns the provider name.
func (p *HyphaeProvider) Name() string { return "hyphae_channel" }

// BindChannel establishes the channel for the given role.
func (p *HyphaeProvider) BindChannel(ctx context.Context, req *BindRequest) error {
	switch req.Role {
	case "listener":
		return p.bindListener(ctx, req)
	case "initiator":
		return p.bindInitiator(ctx, req)
	default:
		return fmt.Errorf("HyphaeProvider.BindChannel: unknown role %q", req.Role)
	}
}

// UnbindChannel tears down an active channel session or connection.
func (p *HyphaeProvider) UnbindChannel(_ context.Context, channelID string) error {
	p.mu.Lock()
	ac, ok := p.active[channelID]
	if ok {
		delete(p.active, channelID)
	}
	p.mu.Unlock()

	if !ok {
		return nil
	}
	ac.cancel()
	if ac.session != nil {
		_ = ac.session.Close()
	}
	if ac.conn != nil {
		_ = ac.conn.Close()
	}
	return nil
}

// Close tears down all active channels.
func (p *HyphaeProvider) Close() error {
	p.mu.Lock()
	ids := make([]string, 0, len(p.active))
	for id := range p.active {
		ids = append(ids, id)
	}
	p.mu.Unlock()

	for _, id := range ids {
		_ = p.UnbindChannel(context.Background(), id)
	}
	return nil
}

// ── listener side ─────────────────────────────────────────────────────────────

func (p *HyphaeProvider) bindListener(ctx context.Context, req *BindRequest) error {
	tunnelAddr := req.HyphaeTunnelAddr
	if tunnelAddr == "" {
		tunnelAddr = p.cfg.TunnelAddr
	}

	tlsCfg, err := p.buildTLS(tunnelAddr)
	if err != nil {
		return fmt.Errorf("bindListener: build TLS: %w", err)
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 15 * time.Second}, "tcp", tunnelAddr, tlsCfg)
	if err != nil {
		return fmt.Errorf("bindListener: dial %s: %w", tunnelAddr, err)
	}

	// HTTP upgrade: register as listener for this org.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+tunnelAddr+"/", nil)
	if err != nil {
		conn.Close()
		return fmt.Errorf("bindListener: build request: %w", err)
	}
	httpReq.Header.Set("Upgrade", "tunnel")
	httpReq.Header.Set("Connection", "Upgrade")
	httpReq.Header.Set("X-Listener-Register", "true")
	httpReq.Header.Set("X-Org-ID", req.OrgID)

	if err := httpReq.Write(conn); err != nil {
		conn.Close()
		return fmt.Errorf("bindListener: write upgrade request: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, httpReq)
	if err != nil {
		conn.Close()
		return fmt.Errorf("bindListener: read upgrade response: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return fmt.Errorf("bindListener: unexpected status %d from Hyphae", resp.StatusCode)
	}

	yamuxCfg := yamux.DefaultConfig()
	yamuxCfg.KeepAliveInterval = 30 * time.Second
	sess, err := yamux.Client(newBufferedConn(conn, br), yamuxCfg)
	if err != nil {
		conn.Close()
		return fmt.Errorf("bindListener: yamux.Client: %w", err)
	}

	bindCtx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.active[req.ChannelID] = &activeChannel{cancel: cancel, session: sess}
	p.mu.Unlock()

	// Background goroutine: accept incoming channel streams from Hyphae.
	go p.acceptLoop(bindCtx, req.ChannelID, sess)

	p.logger.Info("channel listener registered",
		"channel_id", req.ChannelID,
		"org_id", req.OrgID,
		"tunnel_addr", tunnelAddr,
	)
	return nil
}

// acceptLoop accepts yamux streams opened by Hyphae for incoming channels.
// Each stream is a raw net.Conn to the initiator side.
func (p *HyphaeProvider) acceptLoop(ctx context.Context, channelID string, sess *yamux.Session) {
	logger := p.logger.With("channel_id", channelID)
	for {
		stream, err := sess.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
			default:
				logger.Warn("channel listener accept error", "error", err)
			}
			return
		}
		// TODO(UNDF-111): route stream to a local service based on channel purpose.
		logger.Info("channel stream accepted — closing (routing not yet implemented)")
		_ = stream.Close()
	}
}

// ── initiator side ────────────────────────────────────────────────────────────

func (p *HyphaeProvider) bindInitiator(ctx context.Context, req *BindRequest) error {
	if req.Grant == "" {
		return fmt.Errorf("bindInitiator: grant is required for initiator role")
	}

	tunnelAddr := req.HyphaeTunnelAddr
	if tunnelAddr == "" {
		tunnelAddr = p.cfg.TunnelAddr
	}

	tlsCfg, err := p.buildTLS(tunnelAddr)
	if err != nil {
		return fmt.Errorf("bindInitiator: build TLS: %w", err)
	}

	// Retry loop: the listener may not be registered yet when the initiator arrives.
	// Hyphae returns 503 when the listener is not ready; retry with backoff.
	var conn *tls.Conn
	var br *bufio.Reader
	maxAttempts := 10
	backoff := 2 * time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 15 * time.Second}, "tcp", tunnelAddr, tlsCfg)
		if err != nil {
			return fmt.Errorf("bindInitiator: dial %s: %w", tunnelAddr, err)
		}

		httpReq, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+tunnelAddr+"/", nil)
		if reqErr != nil {
			conn.Close()
			return fmt.Errorf("bindInitiator: build request: %w", reqErr)
		}
		httpReq.Header.Set("Upgrade", "tunnel")
		httpReq.Header.Set("Connection", "Upgrade")
		httpReq.Header.Set("X-Channel-ID", req.ChannelID)
		httpReq.Header.Set("X-Channel-Grant", req.Grant)

		if writeErr := httpReq.Write(conn); writeErr != nil {
			conn.Close()
			return fmt.Errorf("bindInitiator: write upgrade request: %w", writeErr)
		}

		br = bufio.NewReader(conn)
		resp, readErr := http.ReadResponse(br, httpReq)
		if readErr != nil {
			conn.Close()
			return fmt.Errorf("bindInitiator: read upgrade response: %w", readErr)
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusSwitchingProtocols {
			break // success
		}

		conn.Close()
		if resp.StatusCode == http.StatusServiceUnavailable && attempt < maxAttempts {
			p.logger.Info("channel initiator: listener not ready, retrying",
				"channel_id", req.ChannelID,
				"attempt", attempt,
				"backoff", backoff,
			)
			select {
			case <-ctx.Done():
				return fmt.Errorf("bindInitiator: context cancelled while waiting for listener")
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 16*time.Second)
			continue
		}

		return fmt.Errorf("bindInitiator: unexpected status %d from Hyphae", resp.StatusCode)
	}

	bindCtx, cancel := context.WithCancel(context.Background())
	rawConn := newBufferedConn(conn, br)

	p.mu.Lock()
	p.active[req.ChannelID] = &activeChannel{cancel: cancel, conn: rawConn}
	p.mu.Unlock()

	p.logger.Info("channel initiator connected",
		"channel_id", req.ChannelID,
		"tunnel_addr", tunnelAddr,
	)
	// TODO(UNDF-111): pipe rawConn to a local service based on channel purpose.
	_ = bindCtx
	return nil
}

// ── TLS helpers ───────────────────────────────────────────────────────────────

// buildTLS constructs a *tls.Config appropriate for connecting to Hyphae.
// If p.tlsCfg is already set (bootstrapped cert), it is cloned and used.
// Otherwise, certs are loaded from the file paths in p.cfg.
func (p *HyphaeProvider) buildTLS(serverAddr string) (*tls.Config, error) {
	if p.tlsCfg != nil {
		c := p.tlsCfg.Clone()
		c.ServerName = hostOnly(serverAddr)
		return c, nil
	}

	tlsCfg := &tls.Config{
		ServerName: hostOnly(serverAddr),
		MinVersion: tls.VersionTLS12,
	}

	// Load client certificate (mTLS).
	if p.cfg.ClientCertPath != "" && p.cfg.ClientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(p.cfg.ClientCertPath, p.cfg.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("buildTLS: load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate (server verification).
	if p.cfg.CACertPath != "" {
		pool := x509.NewCertPool()
		caBytes, err := loadFileBytes(p.cfg.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("buildTLS: read CA cert %s: %w", p.cfg.CACertPath, err)
		}
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("buildTLS: parse CA cert from %s", p.cfg.CACertPath)
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}

func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
