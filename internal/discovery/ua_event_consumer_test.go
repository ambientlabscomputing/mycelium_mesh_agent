package discovery

import (
	"context"
	"testing"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	mmtypes "github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

func TestHandleEvent_UsesRegisteredCustomHandler(t *testing.T) {
	ctx, _ := logging.Init(context.Background(), logging.LoggerModeDev, nil)

	ec := NewEventConsumer(NewRegistry(), config.ControlChannelConfig{})

	handled := false
	ec.RegisterHandler(mmtypes.EventTunnelBindRequested, func(ctx context.Context, event *mmtypes.UAEvent) error {
		handled = true
		return nil
	})

	err := ec.HandleEvent(ctx, &mmtypes.UAEvent{
		EventID:   "evt-1",
		EventType: mmtypes.EventTunnelBindRequested,
		NodeID:    "node-1",
		Seq:       1,
		Payload: map[string]interface{}{
			"tunnel_id": "tun-1",
		},
	})
	if err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected registered handler to be invoked")
	}
}
