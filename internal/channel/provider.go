package channel

import "context"

// Provider establishes and manages relay channel connections to Hyphae.
// A channel is a peer-to-peer relay — one side acts as listener (destination
// server) and the other as initiator (source server).
type Provider interface {
	Name() string
	BindChannel(ctx context.Context, req *BindRequest) error
	UnbindChannel(ctx context.Context, channelID string) error
	Close() error
}

// BindRequest carries the parameters forwarded in a channel.bind.request event.
type BindRequest struct {
	ChannelID        string
	OrgID            string
	Role             string // "listener" | "initiator"
	Grant            string // ES256 JWT; only present for "initiator"
	HyphaeTunnelAddr string // overrides provider config when set
	SourceServerID   string
	DestServerID     string
	Purpose          string
}
