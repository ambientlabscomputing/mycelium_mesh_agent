package server

// API request limits
const (
	MaxRequestBodySize    = 1 * 1024 * 1024 // 1 MB
	MaxServiceIDLength    = 256
	MaxCapabilityIDLength = 256
	MaxBindingIDLength    = 128
	MaxRequestIDLength    = 128
	MaxTypesArrayLength   = 100
	MaxTypeNameLength     = 64
)
