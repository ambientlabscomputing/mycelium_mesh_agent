package channel

import (
	"log/slog"
	"testing"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
)

func newTestProvider(cfg config.HyphaeConfig) *HyphaeProvider {
	return NewHyphaeProvider(cfg, slog.Default())
}

// ---------------------------------------------------------------------------
// resolveLocalAddr
// ---------------------------------------------------------------------------

func TestResolveLocalAddr_DynamicRoutePrefixMatch(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})
	p.RegisterRoute("secret-replication", "127.0.0.1:46749")

	// Exact match on the prefix.
	addr, err := p.resolveLocalAddr("secret-replication")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:46749" {
		t.Errorf("addr = %q, want 127.0.0.1:46749", addr)
	}

	// Prefix + colon-separated suffix (the real-world pattern:
	// purpose = "secret-replication:<secret_id>").
	addr, err = p.resolveLocalAddr("secret-replication:c4f5165e-7020-4842-83d4-6012636bc359")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:46749" {
		t.Errorf("addr = %q, want 127.0.0.1:46749", addr)
	}
}

func TestResolveLocalAddr_DynamicRouteDoesNotMatchSubstring(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})
	p.RegisterRoute("secret-replication", "127.0.0.1:46749")

	// A purpose that merely contains the prefix as a substring must NOT match.
	_, err := p.resolveLocalAddr("not-secret-replication:abc")
	if err == nil {
		t.Fatal("expected error for non-matching prefix, got nil")
	}
}

func TestResolveLocalAddr_StaticChannelRouteExactMatch(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{
		ChannelRoutes: map[string]string{
			"tunnel:ssh": "127.0.0.1:22",
		},
	})

	addr, err := p.resolveLocalAddr("tunnel:ssh")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:22" {
		t.Errorf("addr = %q, want 127.0.0.1:22", addr)
	}
}

func TestResolveLocalAddr_DynamicTakesPrecedenceOverStatic(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{
		ChannelRoutes: map[string]string{
			"secret-replication": "127.0.0.1:11111",
		},
	})
	// Dynamic registration should override static config.
	p.RegisterRoute("secret-replication", "127.0.0.1:22222")

	addr, err := p.resolveLocalAddr("secret-replication:some-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:22222" {
		t.Errorf("addr = %q, want 127.0.0.1:22222 (dynamic)", addr)
	}
}

func TestResolveLocalAddr_RawHostPortPassthrough(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})

	addr, err := p.resolveLocalAddr("127.0.0.1:5000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:5000" {
		t.Errorf("addr = %q, want 127.0.0.1:5000", addr)
	}
}

func TestResolveLocalAddr_UnknownPurposeReturnsError(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})

	_, err := p.resolveLocalAddr("unknown-service:abc-123")
	if err == nil {
		t.Fatal("expected error for unknown purpose, got nil")
	}
}

// ---------------------------------------------------------------------------
// RegisterRoute
// ---------------------------------------------------------------------------

func TestRegisterRoute_OverwritesPreviousValue(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})

	p.RegisterRoute("secret-replication", "127.0.0.1:11111")
	p.RegisterRoute("secret-replication", "127.0.0.1:22222")

	addr, err := p.resolveLocalAddr("secret-replication:id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:22222" {
		t.Errorf("addr = %q, want 127.0.0.1:22222 (overwritten)", addr)
	}
}

func TestRegisterRoute_MultipleDistinctPrefixes(t *testing.T) {
	p := newTestProvider(config.HyphaeConfig{})
	p.RegisterRoute("secret-replication", "127.0.0.1:46749")
	p.RegisterRoute("log-stream", "127.0.0.1:50000")

	addr1, err := p.resolveLocalAddr("secret-replication:xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr1 != "127.0.0.1:46749" {
		t.Errorf("addr = %q, want 127.0.0.1:46749", addr1)
	}

	addr2, err := p.resolveLocalAddr("log-stream:abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr2 != "127.0.0.1:50000" {
		t.Errorf("addr = %q, want 127.0.0.1:50000", addr2)
	}
}
