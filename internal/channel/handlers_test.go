package channel

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	mmtypes "github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

func init() {
	// Initialize logger so handlers don't panic when calling logging.GetLogger.
	logging.Init(context.Background(), logging.LoggerModeDev, nil)
}

// mockChannelProvider is a test double for the Provider interface.
type mockChannelProvider struct {
	name       string
	bindCalled bool
	bindReq    *BindRequest
	bindErr    error

	unbindCalled bool
	unbindID     string
	unbindErr    error
}

func (m *mockChannelProvider) Name() string { return m.name }

func (m *mockChannelProvider) BindChannel(_ context.Context, req *BindRequest) error {
	m.bindCalled = true
	m.bindReq = req
	return m.bindErr
}

func (m *mockChannelProvider) UnbindChannel(_ context.Context, channelID string) error {
	m.unbindCalled = true
	m.unbindID = channelID
	return m.unbindErr
}

func (m *mockChannelProvider) LocalAddr(_ string) (string, bool) { return "", false }

func (m *mockChannelProvider) Close() error { return nil }

func makeChannelBindEvent(channelID, orgID, role, grant, srcID, dstID string) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-ch-001",
		EventType: mmtypes.EventChannelBindRequested,
		Payload: map[string]interface{}{
			"channel_id":         channelID,
			"org_id":             orgID,
			"role":               role,
			"grant":              grant,
			"source_server_id":   srcID,
			"dest_server_id":     dstID,
			"purpose":            "test-purpose",
			"hyphae_tunnel_addr": "hyphae.example.com:9090",
			"expires_at":         int64(9999999999),
			"created_at":         int64(1000000000),
		},
	}
}

func TestHandleChannelBindRequested_ListenerSuccess(t *testing.T) {
	provider := &mockChannelProvider{}
	event := makeChannelBindEvent("ch-001", "org-1", "listener", "", "srv-a", "srv-b")

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.bindCalled {
		t.Error("expected BindChannel to be called")
	}
	if provider.bindReq == nil {
		t.Fatal("bindReq is nil")
	}
	if provider.bindReq.ChannelID != "ch-001" {
		t.Errorf("ChannelID = %q, want ch-001", provider.bindReq.ChannelID)
	}
	if provider.bindReq.Role != "listener" {
		t.Errorf("Role = %q, want listener", provider.bindReq.Role)
	}
}

func TestHandleChannelBindRequested_InitiatorSuccess(t *testing.T) {
	provider := &mockChannelProvider{}
	event := makeChannelBindEvent("ch-002", "org-1", "initiator", "jwt-token-xyz", "srv-a", "srv-b")

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.bindCalled {
		t.Error("expected BindChannel to be called")
	}
	if provider.bindReq.Grant != "jwt-token-xyz" {
		t.Errorf("Grant = %q, want jwt-token-xyz", provider.bindReq.Grant)
	}
	if provider.bindReq.Role != "initiator" {
		t.Errorf("Role = %q, want initiator", provider.bindReq.Role)
	}
}

func TestHandleChannelBindRequested_ProviderError(t *testing.T) {
	bindErr := errors.New("hyphae dial failed")
	provider := &mockChannelProvider{bindErr: bindErr}
	event := makeChannelBindEvent("ch-003", "org-1", "listener", "", "srv-a", "srv-b")

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, bindErr) {
		t.Errorf("expected bindErr, got %v", err)
	}
	if !provider.bindCalled {
		t.Error("BindChannel must be called even when it returns an error")
	}
}

func TestHandleChannelBindRequested_MalformedPayload(t *testing.T) {
	provider := &mockChannelProvider{}
	event := &mmtypes.UAEvent{
		EventID:   "evt-bad",
		EventType: mmtypes.EventChannelBindRequested,
		Payload:   make(chan int),
	}

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
	if provider.bindCalled {
		t.Error("BindChannel must not be called on malformed payload")
	}
}

func TestHandleChannelBindRequested_NilEmitter_NoError(t *testing.T) {
	provider := &mockChannelProvider{}
	event := makeChannelBindEvent("ch-004", "org-1", "listener", "", "srv-a", "srv-b")

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Errorf("unexpected error with nil emitter: %v", err)
	}
}

func TestHandleChannelBindRequested_ProviderError_NilEmitter(t *testing.T) {
	bindErr := errors.New("relay unavailable")
	provider := &mockChannelProvider{bindErr: bindErr}
	event := makeChannelBindEvent("ch-005", "org-1", "initiator", "grant-token", "srv-a", "srv-b")

	err := handleChannelBindRequested(context.Background(), provider, nil, event)
	if !errors.Is(err, bindErr) {
		t.Errorf("expected bindErr, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// handleChannelRouteRegister tests
// ---------------------------------------------------------------------------

func makeRouteRegisterEvent(purpose, addr string) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-route-001",
		EventType: mmtypes.EventChannelRouteRegister,
		Payload: map[string]interface{}{
			"purpose": purpose,
			"addr":    addr,
		},
	}
}

func TestHandleChannelRouteRegister_Success(t *testing.T) {
	p := NewHyphaeProvider(config.HyphaeConfig{}, slog.Default())
	event := makeRouteRegisterEvent("secret-replication", "127.0.0.1:46749")

	err := handleChannelRouteRegister(context.Background(), p, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the route was actually registered by resolving it.
	addr, resolveErr := p.resolveLocalAddr("secret-replication:some-uuid")
	if resolveErr != nil {
		t.Fatalf("resolveLocalAddr failed after registration: %v", resolveErr)
	}
	if addr != "127.0.0.1:46749" {
		t.Errorf("addr = %q, want 127.0.0.1:46749", addr)
	}
}

func TestHandleChannelRouteRegister_MissingPurpose(t *testing.T) {
	p := NewHyphaeProvider(config.HyphaeConfig{}, slog.Default())
	event := makeRouteRegisterEvent("", "127.0.0.1:46749")

	err := handleChannelRouteRegister(context.Background(), p, event)
	if err == nil {
		t.Fatal("expected error for empty purpose, got nil")
	}
}

func TestHandleChannelRouteRegister_MissingAddr(t *testing.T) {
	p := NewHyphaeProvider(config.HyphaeConfig{}, slog.Default())
	event := makeRouteRegisterEvent("secret-replication", "")

	err := handleChannelRouteRegister(context.Background(), p, event)
	if err == nil {
		t.Fatal("expected error for empty addr, got nil")
	}
}

func TestHandleChannelRouteRegister_MalformedPayload(t *testing.T) {
	p := NewHyphaeProvider(config.HyphaeConfig{}, slog.Default())
	event := &mmtypes.UAEvent{
		EventID:   "evt-route-bad",
		EventType: mmtypes.EventChannelRouteRegister,
		Payload:   make(chan int), // unmarshalable
	}

	err := handleChannelRouteRegister(context.Background(), p, event)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
}
