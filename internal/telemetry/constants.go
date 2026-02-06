package telemetry

// Buffer configuration constants
const (
	// DefaultBufferSize is the default maximum size for telemetry buffer in bytes
	DefaultBufferSize = 10 * 1024 * 1024 // 10 MB

	// DefaultFlushInterval is how often to flush telemetry to UA (milliseconds)
	DefaultFlushInterval = 30000 // 30 seconds

	// MinFlushInterval is the minimum allowed flush interval
	MinFlushInterval = 5000 // 5 seconds

	// MaxBufferSize is the maximum allowed buffer size
	MaxBufferSize = 100 * 1024 * 1024 // 100 MB
)
