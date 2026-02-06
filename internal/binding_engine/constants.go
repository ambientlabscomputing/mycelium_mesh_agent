package binding_engine

import "time"

// Constants for binding engine configuration
const (
	// DefaultGrantTTL is the default time-to-live for binding grants
	DefaultGrantTTL = 1 * time.Hour

	// ShortGrantTTL is used for temporary bindings (testing, development)
	ShortGrantTTL = 10 * time.Second

	// GrantRenewalWindow is how far before expiration we allow renewal
	GrantRenewalWindow = 5 * time.Minute

	// ExpirationCheckInterval is how often to check for expired grants
	ExpirationCheckInterval = 30 * time.Second
)

// MaxFieldLengths defines maximum lengths for various fields
const (
	MaxServiceIDLength    = 256
	MaxCapabilityIDLength = 256
	MaxBindingIDLength    = 128
	MaxIdentityLength     = 512
)
