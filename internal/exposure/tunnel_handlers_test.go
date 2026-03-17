package exposure

import (
	"context"
	"errors"
	"testing"

	mmtypes "github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// makeTunnelBindEvent builds a UAEvent for a tunnel bind request.
func makeTunnelBindEvent(tunnelID, leaseID, hostname, target, targetType string) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-tun-001",
		EventType: mmtypes.EventTunnelBindRequested,
		Payload: map[string]interface{}{
			"tunnel_id":          tunnelID,
			"lease_id":           leaseID,
			"hostname":           hostname,
			"target":             target,
			"target_type":        targetType,
			"hyphae_tunnel_addr": "hyphae.internal:9000",
		},
	}
}

// makeTunnelUnbindEvent builds a UAEvent for a tunnel unbind request.
func makeTunnelUnbindEvent(tunnelID, leaseID string) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-tun-002",
		EventType: mmtypes.EventTunnelUnbindRequested,
		Payload: map[string]interface{}{
			"tunnel_id": tunnelID,
			"lease_id":  leaseID,
		},
	}
}

// ===================== handleTunnelBindRequested =====================

func TestHandleTunnelBindRequested_PortTarget_Success(t *testing.T) {
	provider := &mockProvider{}
	event := makeTunnelBindEvent("tun-001", "lease-001", "tunnel-aabb.underleafapp.com", "8080", "port")

	err := handleTunnelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.bindCalled {
		t.Fatal("expected Bind to be called")
	}
	if provider.bindReq.ExposureID != "tun-001" {
		t.Errorf("ExposureID = %q, want tun-001", provider.bindReq.ExposureID)
	}
	// port target: LocalAddr should be "localhost:8080", TargetURL empty
	if provider.bindReq.LocalAddr != "localhost:8080" {
		t.Errorf("LocalAddr = %q, want localhost:8080", provider.bindReq.LocalAddr)
	}
	if provider.bindReq.TargetURL != "" {
		t.Errorf("TargetURL = %q, want empty for port target", provider.bindReq.TargetURL)
	}
}

func TestHandleTunnelBindRequested_URLTarget_Success(t *testing.T) {
	provider := &mockProvider{}
	event := makeTunnelBindEvent("tun-002", "lease-002", "tunnel-ccdd.underleafapp.com", "http://192.168.1.50:3000", "url")

	err := handleTunnelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.bindCalled {
		t.Fatal("expected Bind to be called")
	}
	// url target: TargetURL set, LocalAddr empty
	if provider.bindReq.TargetURL != "http://192.168.1.50:3000" {
		t.Errorf("TargetURL = %q, want http://192.168.1.50:3000", provider.bindReq.TargetURL)
	}
	if provider.bindReq.LocalAddr != "" {
		t.Errorf("LocalAddr = %q, want empty for url target", provider.bindReq.LocalAddr)
	}
}

func TestHandleTunnelBindRequested_ProviderError(t *testing.T) {
	bindErr := errors.New("hyphae connect failed")
	provider := &mockProvider{bindErr: bindErr}
	event := makeTunnelBindEvent("tun-003", "lease-003", "tunnel-eeff.underleafapp.com", "9090", "port")

	err := handleTunnelBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, bindErr) {
		t.Errorf("expected bindErr, got %v", err)
	}
	if !provider.bindCalled {
		t.Error("Bind should still be called even on error")
	}
}

func TestHandleTunnelBindRequested_MalformedPayload(t *testing.T) {
	provider := &mockProvider{}
	event := &mmtypes.UAEvent{
		EventID:   "evt-bad",
		EventType: mmtypes.EventTunnelBindRequested,
		Payload:   make(chan int), // not serialisable
	}

	err := handleTunnelBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
	if provider.bindCalled {
		t.Error("Bind must not be called on malformed payload")
	}
}

func TestHandleTunnelBindRequested_NilEmitter(t *testing.T) {
	provider := &mockProvider{}
	event := makeTunnelBindEvent("tun-004", "lease-004", "tunnel-0011.underleafapp.com", "3000", "port")

	// nil emitter must not cause a panic
	err := handleTunnelBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Errorf("unexpected error with nil emitter: %v", err)
	}
}

// ===================== handleTunnelUnbindRequested =====================

func TestHandleTunnelUnbindRequested_Success(t *testing.T) {
	provider := &mockProvider{}
	event := makeTunnelUnbindEvent("tun-001", "lease-001")

	err := handleTunnelUnbindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.unbindCalled {
		t.Fatal("expected Unbind to be called")
	}
	if provider.unbindID != "tun-001" {
		t.Errorf("unbindID = %q, want tun-001", provider.unbindID)
	}
}

func TestHandleTunnelUnbindRequested_ProviderError(t *testing.T) {
	unbindErr := errors.New("tunnel not found")
	provider := &mockProvider{unbindErr: unbindErr}
	event := makeTunnelUnbindEvent("tun-002", "lease-002")

	err := handleTunnelUnbindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, unbindErr) {
		t.Errorf("expected unbindErr, got %v", err)
	}
}

func TestHandleTunnelUnbindRequested_MalformedPayload(t *testing.T) {
	provider := &mockProvider{}
	event := &mmtypes.UAEvent{
		EventID:   "evt-bad-unbind",
		EventType: mmtypes.EventTunnelUnbindRequested,
		Payload:   make(chan int),
	}

	err := handleTunnelUnbindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
	if provider.unbindCalled {
		t.Error("Unbind must not be called on malformed payload")
	}
}

func TestHandleTunnelUnbindRequested_NilEmitter(t *testing.T) {
	provider := &mockProvider{}
	event := makeTunnelUnbindEvent("tun-005", "lease-005")

	err := handleTunnelUnbindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Errorf("unexpected error with nil emitter: %v", err)
	}
}
