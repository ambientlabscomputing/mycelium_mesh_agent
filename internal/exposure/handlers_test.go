package exposure

import (
	"context"
	"errors"
	"testing"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	mmtypes "github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

func init() {
	// Initialize logger so handlers don't panic when calling logging.GetLogger
	logging.Init(context.Background(), logging.LoggerModeDev, nil)
}

// mockProvider is a test double for the Provider interface.
type mockProvider struct {
	name       string
	bindCalled bool
	bindReq    *BindRequest
	bindResult *BindResult
	bindErr    error

	unbindCalled bool
	unbindID     string
	unbindErr    error
}

func (m *mockProvider) Name() string { return m.name }

func (m *mockProvider) Bind(ctx context.Context, req *BindRequest) (*BindResult, error) {
	m.bindCalled = true
	m.bindReq = req
	if m.bindErr != nil {
		return nil, m.bindErr
	}
	if m.bindResult != nil {
		return m.bindResult, nil
	}
	return &BindResult{
		ExposureID: req.ExposureID,
		LeaseID:    req.LeaseID,
		PublicURL:  "https://web-abc12345.underleafapp.com",
		Status:     "bound",
	}, nil
}

func (m *mockProvider) Unbind(ctx context.Context, exposureID string) error {
	m.unbindCalled = true
	m.unbindID = exposureID
	return m.unbindErr
}

func (m *mockProvider) Status(exposureID string) (string, error) { return "bound", nil }
func (m *mockProvider) Close() error                             { return nil }

// makeBindEvent builds a UAEvent with an ExposureBindRequestedPayload.
func makeBindEvent(exposureID, leaseID, hostname string, targetPort int) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-001",
		EventType: mmtypes.EventExposureBindRequested,
		Payload: map[string]interface{}{
			"exposure_id": exposureID,
			"lease_id":    leaseID,
			"hostname":    hostname,
			"target_port": targetPort,
			"local_addr":  "127.0.0.1:8080",
		},
	}
}

// makeUnbindEvent builds a UAEvent with an ExposureUnbindRequestedPayload.
func makeUnbindEvent(exposureID, leaseID string) *mmtypes.UAEvent {
	return &mmtypes.UAEvent{
		EventID:   "evt-002",
		EventType: mmtypes.EventExposureUnbindRequested,
		Payload: map[string]interface{}{
			"exposure_id": exposureID,
			"lease_id":    leaseID,
		},
	}
}

// handleExposureBindRequested tests

func TestHandleExposureBindRequested_Success(t *testing.T) {
	provider := &mockProvider{}
	event := makeBindEvent("exp-001", "lease-001", "web-abc12345", 8080)

	err := handleExposureBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.bindCalled {
		t.Error("expected Bind to be called")
	}
	if provider.bindReq == nil {
		t.Fatal("bindReq is nil")
	}
	if provider.bindReq.ExposureID != "exp-001" {
		t.Errorf("expected ExposureID=exp-001, got %q", provider.bindReq.ExposureID)
	}
	if provider.bindReq.TargetPort != 8080 {
		t.Errorf("expected TargetPort=8080, got %d", provider.bindReq.TargetPort)
	}
}

func TestHandleExposureBindRequested_ProviderError(t *testing.T) {
	bindErr := errors.New("tunnel dial failed")
	provider := &mockProvider{bindErr: bindErr}
	event := makeBindEvent("exp-002", "lease-002", "api-defgh", 3000)

	err := handleExposureBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, bindErr) {
		t.Errorf("expected bindErr, got %v", err)
	}
	if !provider.bindCalled {
		t.Error("expected Bind to have been called even on error")
	}
}

func TestHandleExposureBindRequested_MalformedPayload(t *testing.T) {
	provider := &mockProvider{}
	// Pass a channel as payload; json.Marshal will fail on it
	event := &mmtypes.UAEvent{
		EventID:   "evt-bad",
		EventType: mmtypes.EventExposureBindRequested,
		Payload:   make(chan int),
	}

	err := handleExposureBindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
	if provider.bindCalled {
		t.Error("Bind must not be called on malformed payload")
	}
}

func TestHandleExposureBindRequested_NilEmitter_NoError(t *testing.T) {
	provider := &mockProvider{}
	event := makeBindEvent("exp-003", "lease-003", "svc-xyz12345", 5000)

	err := handleExposureBindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Errorf("unexpected error with nil emitter: %v", err)
	}
}

// handleExposureUnbindRequested tests

func TestHandleExposureUnbindRequested_Success(t *testing.T) {
	provider := &mockProvider{}
	event := makeUnbindEvent("exp-004", "lease-004")

	err := handleExposureUnbindRequested(context.Background(), provider, nil, event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provider.unbindCalled {
		t.Error("expected Unbind to be called")
	}
	if provider.unbindID != "exp-004" {
		t.Errorf("expected unbindID=exp-004, got %q", provider.unbindID)
	}
}

func TestHandleExposureUnbindRequested_ProviderError(t *testing.T) {
	unbindErr := errors.New("failed to close tunnel")
	provider := &mockProvider{unbindErr: unbindErr}
	event := makeUnbindEvent("exp-005", "lease-005")

	err := handleExposureUnbindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, unbindErr) {
		t.Errorf("expected unbindErr, got %v", err)
	}
}

func TestHandleExposureUnbindRequested_MalformedPayload(t *testing.T) {
	provider := &mockProvider{}
	event := &mmtypes.UAEvent{
		EventID:   "evt-bad2",
		EventType: mmtypes.EventExposureUnbindRequested,
		Payload:   make(chan int),
	}

	err := handleExposureUnbindRequested(context.Background(), provider, nil, event)
	if err == nil {
		t.Fatal("expected error for malformed payload")
	}
	if provider.unbindCalled {
		t.Error("Unbind must not be called on malformed payload")
	}
}
