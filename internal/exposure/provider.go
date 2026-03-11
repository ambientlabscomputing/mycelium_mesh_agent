package exposure

import (
	"context"
)

// Provider represents a service exposure backend (e.g., Hyphae, Cloudflare Tunnel)
type Provider interface {
	Name() string
	Bind(ctx context.Context, req *BindRequest) (*BindResult, error)
	Unbind(ctx context.Context, exposureID string) error
	Status(exposureID string) (string, error)
	Close() error
}

type BindRequest struct {
	ExposureID string
	LeaseID    string
	Hostname   string
	TargetPort int
	LocalAddr  string
	// TunnelAddr, when non-empty, overrides the provider's static tunnel address
	// for this specific exposure. Populated from the hyphae_tunnel_addr field in
	// the exposure.bind.request Spine event so that server_api controls which
	// Hyphae instance each exposure connects to.
	TunnelAddr string
}

type BindResult struct {
	ExposureID string
	LeaseID    string
	PublicURL  string
	Status     string
}

type ExposureStatus struct {
	ExposureID string
	Status     string
	LastUpdate string
	Error      string
}
