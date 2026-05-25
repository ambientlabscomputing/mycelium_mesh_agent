package channel

import (
	"context"
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
