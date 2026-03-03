// Package kernel provides a client for the UA kernel syscall server.
package kernel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	pb "github.com/ambientlabscomputing/umc_sdk/proto/ua_kernel/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// IdentityClient wraps the kernel IdentityService so MMA can request
// certificates and node identity information.
type IdentityClient struct {
	identityClient pb.IdentityServiceClient
	logger         *slog.Logger
}

// NewIdentityClient connects to the kernel syscall socket and returns an identity client.
// socketPath defaults to /tmp/ua_kernel.sock if empty.
func NewIdentityClient(socketPath string, logger *slog.Logger) (*IdentityClient, error) {
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

	return &IdentityClient{
		identityClient: pb.NewIdentityServiceClient(conn),
		logger:         logger.With("component", "kernel_identity"),
	}, nil
}

// IssueLocalCertificate requests a certificate from the kernel's IdentityService.
// The kernel will generate a CSR and submit it to server_api for signing.
//
// Arguments:
//   - ctx: context for the RPC call
//   - componentName: name of the requesting component (e.g., "mma")
//   - dnsNames: optional list of DNS SANs for the certificate
//   - ipAddresses: optional list of IP SANs for the certificate
//   - validityDays: certificate validity period in days
//
// Returns:
//   - certificatePEM: the signed certificate in PEM format
//   - privateKeyPEM: the private key in PEM format
//   - expiresAt: expiration timestamp
//   - error: if the request failed
func (c *IdentityClient) IssueLocalCertificate(
	ctx context.Context,
	componentName string,
	dnsNames []string,
	ipAddresses []string,
	validityDays uint32,
) (certificatePEM, privateKeyPEM string, expiresAt *time.Time, err error) {
	c.logger.Info("requesting local certificate", "component", componentName)

	req := &pb.IssueLocalCertificateRequest{
		ComponentName: componentName,
		DnsNames:      dnsNames,
		IpAddresses:   ipAddresses,
		ValidityDays:  validityDays,
	}

	resp, err := c.identityClient.IssueLocalCertificate(ctx, req)
	if err != nil {
		c.logger.Error("IssueLocalCertificate failed", "component", componentName, "error", err)
		return "", "", nil, fmt.Errorf("kernel IssueLocalCertificate failed: %w", err)
	}

	// Parse expiresAt if present
	var parsedTime *time.Time
	if resp.ExpiresAt != nil {
		t := resp.ExpiresAt.AsTime()
		parsedTime = &t
	}

	c.logger.Info("certificate issued successfully", "component", componentName, "expires_at", parsedTime)
	return resp.CertificatePem, resp.PrivateKeyPem, parsedTime, nil
}

// GetNodeIdentity retrieves the current node identity from the kernel.
// This includes the node ID, certificate, and certificate chain.
func (c *IdentityClient) GetNodeIdentity(ctx context.Context) (*pb.GetNodeIdentityResponse, error) {
	c.logger.Info("retrieving node identity")

	resp, err := c.identityClient.GetNodeIdentity(ctx, &pb.GetNodeIdentityRequest{})
	if err != nil {
		c.logger.Error("GetNodeIdentity failed", "error", err)
		return nil, fmt.Errorf("kernel GetNodeIdentity failed: %w", err)
	}

	c.logger.Info("node identity retrieved", "node_id", resp.NodeId)
	return resp, nil
}
