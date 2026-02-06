package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"google.golang.org/grpc/credentials"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
)

// TLSCredentials manages gRPC transport credentials with hot-reload support.
type TLSCredentials struct {
	mu             sync.RWMutex
	config         *tls.Config
	certFile       string
	keyFile        string
	caFile         string
	lastTrustRoots string // for change detection
}

// NewTLSCredentials creates a new TLS credentials manager from ControlChannelConfig.
func NewTLSCredentials(cfg config.ControlChannelConfig) (*TLSCredentials, error) {
	tc := &TLSCredentials{
		certFile: cfg.TLSCertRef,
		keyFile:  cfg.TLSKeyRef,
		caFile:   cfg.TLSCARef,
	}

	if err := tc.reload(); err != nil {
		return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
	}

	return tc, nil
}

// reload reloads certificates and CA from disk.
func (tc *TLSCredentials) reload() error {
	// Load client certificate and key
	cert, err := tls.LoadX509KeyPair(tc.certFile, tc.keyFile)
	if err != nil {
		return fmt.Errorf("failed to load certificate pair: %w", err)
	}

	// Load CA certificate
	caCert, err := os.ReadFile(tc.caFile)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to parse CA certificate")
	}

	// Build TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}

	tc.mu.Lock()
	tc.config = tlsConfig
	tc.lastTrustRoots = string(caCert)
	tc.mu.Unlock()

	return nil
}

// UpdateTrustRoots updates the CA certificate pool with new trust roots.
// This supports hot-reload of trust roots from identity.trust_roots.updated events.
func (tc *TLSCredentials) UpdateTrustRoots(trustBundle string) error {
	tc.mu.RLock()
	if tc.lastTrustRoots == trustBundle {
		tc.mu.RUnlock()
		return nil // No change
	}
	tc.mu.RUnlock()

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(trustBundle)) {
		return fmt.Errorf("failed to parse trust bundle")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Create new config with updated CA pool, preserving existing cert
	tc.config = &tls.Config{
		Certificates: tc.config.Certificates,
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}
	tc.lastTrustRoots = trustBundle

	return nil
}

// GetTransportCredentials returns gRPC transport credentials.
func (tc *TLSCredentials) GetTransportCredentials() credentials.TransportCredentials {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return credentials.NewTLS(tc.config)
}

// GetTLSConfig returns the underlying tls.Config for inspection.
func (tc *TLSCredentials) GetTLSConfig() *tls.Config {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.config.Clone()
}

// BuildTransportCredentials creates gRPC transport credentials from ControlChannelConfig.
// Returns nil if TLS is disabled (for UDS).
func BuildTransportCredentials(cfg config.ControlChannelConfig) (credentials.TransportCredentials, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	if cfg.TLSCertRef == "" || cfg.TLSKeyRef == "" || cfg.TLSCARef == "" {
		return nil, fmt.Errorf("TLS enabled but cert/key/CA refs not configured")
	}

	tc, err := NewTLSCredentials(cfg)
	if err != nil {
		return nil, err
	}

	return tc.GetTransportCredentials(), nil
}
