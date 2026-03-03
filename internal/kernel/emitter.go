// Package kernel provides a client for the UA kernel syscall server.
// MMA uses it to emit lifecycle events back to the kernel (and from
// there, via Spine, to the rest of the platform).
package kernel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	pb "github.com/ambientlabscomputing/umc_sdk/proto/ua_kernel/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// KernelEmitter wraps the kernel EventService so MMA can publish events
// back through the UA kernel (which then forwards them via Spine).
type KernelEmitter struct {
	eventClient pb.EventServiceClient
	logger      *slog.Logger
}

// NewKernelEmitter connects to the kernel syscall socket and returns an emitter.
// socketPath defaults to /tmp/ua_kernel.sock if empty.
func NewKernelEmitter(socketPath string, logger *slog.Logger) (*KernelEmitter, error) {
	if socketPath == "" {
		socketPath = os.Getenv("KERNEL_SOCKET")
	}
	if socketPath == "" {
		socketPath = "/tmp/ua_kernel.sock"
	}
	if logger == nil {
		logger = slog.Default()
	}

	conn, err := grpc.Dial( //nolint:staticcheck
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kernel socket %s: %w", socketPath, err)
	}

	return &KernelEmitter{
		eventClient: pb.NewEventServiceClient(conn),
		logger:      logger.With("component", "kernel_emitter"),
	}, nil
}

// EmitEvent publishes a generic event through the kernel.
func (e *KernelEmitter) EmitEvent(ctx context.Context, eventType, entityKind, entityID string, payload map[string]interface{}) error {
	payloadStruct, err := structpb.NewStruct(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	req := &pb.EmitEventRequest{
		EventType:  eventType,
		EntityKind: entityKind,
		EntityId:   entityID,
		Payload:    payloadStruct,
	}

	if _, err := e.eventClient.EmitEvent(ctx, req); err != nil {
		return fmt.Errorf("kernel EmitEvent failed: %w", err)
	}

	e.logger.Info("event emitted via kernel", "event_type", eventType, "entity_id", entityID)
	return nil
}
