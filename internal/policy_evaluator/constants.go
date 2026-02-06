package policy_evaluator

// Rate limiting constants
const (
	// MaxRPSPerClient is the maximum requests per second allowed per client
	MaxRPSPerClient = 1000

	// MaxRPSPerCapability is the maximum RPS per capability
	MaxRPSPerCapability = 5000

	// RateLimitWindowSeconds is the time window for rate limiting
	RateLimitWindowSeconds = 60
)
