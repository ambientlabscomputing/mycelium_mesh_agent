package types

import (
	"errors"
)

// Sentinel Errors
var (
	ErrCapabilityNotFound     = errors.New("capability not found in cache")
	ErrNoProviderAvailable    = errors.New("no provider available for capability")
	ErrPolicyDenied           = errors.New("policy denied the binding")
	ErrConsentRequired        = errors.New("consent required from user/app")
	ErrTrustTierInsufficient  = errors.New("provider trust tier insufficient")
	ErrProvenanceMismatch     = errors.New("provider provenance does not match")
	ErrLocalityViolation      = errors.New("locality constraint violated")
	ErrRateLimited            = errors.New("rate limit exceeded")
	ErrInvalidConfig          = errors.New("invalid configuration")
	ErrInvalidPolicy          = errors.New("invalid policy")
	ErrMissingConfigRequired  = errors.New("missing required config field")
	ErrConfigVersionMismatch  = errors.New("config version mismatch")
	ErrMemberNotFound         = errors.New("member not found in cluster")
	ErrServiceNotFound        = errors.New("service not found")
	ErrInvalidNodeIdentity    = errors.New("invalid node identity")
	ErrPolicyNotFound         = errors.New("policy not found or not loaded")
	ErrPolicyEvaluationFailed = errors.New("policy evaluation failed")
	ErrInvalidPolicyDocument  = errors.New("invalid policy document")
	ErrTelemetryBufferFull    = errors.New("telemetry buffer full")
	ErrFailedToFlushTelemetry = errors.New("failed to flush telemetry to UA")
	ErrInvalidBindingRequest  = errors.New("invalid binding request")
	ErrBindingNotFound        = errors.New("binding not found")
	ErrServiceUnhealthy       = errors.New("service is unhealthy")
	ErrServiceOffline         = errors.New("service is offline")
	ErrConnectionFailed       = errors.New("connection to service failed")
	ErrMTLSVerificationFailed = errors.New("mTLS verification failed")
)

// ErrorCode represents a machine-readable error code
type ErrorCode string

const (
	ErrorCapabilityUnknown      ErrorCode = "CAPABILITY_UNKNOWN"
	ErrorNoProviderAvailable    ErrorCode = "NO_PROVIDER_AVAILABLE"
	ErrorPolicyDenied           ErrorCode = "POLICY_DENIED"
	ErrorConsentRequired        ErrorCode = "CONSENT_REQUIRED"
	ErrorTrustTierInsufficient  ErrorCode = "TRUST_TIER_INSUFFICIENT"
	ErrorProvenanceMismatch     ErrorCode = "PROVENANCE_MISMATCH"
	ErrorLocalityViolation      ErrorCode = "LOCALITY_VIOLATION"
	ErrorRateLimited            ErrorCode = "RATE_LIMITED"
	ErrorInvalidConfig          ErrorCode = "INVALID_CONFIG"
	ErrorMissingConfigRequired  ErrorCode = "MISSING_REQUIRED_CONFIG"
	ErrorConfigVersionMismatch  ErrorCode = "CONFIG_VERSION_MISMATCH"
	ErrorMemberNotFound         ErrorCode = "MEMBER_NOT_FOUND"
	ErrorServiceNotFound        ErrorCode = "SERVICE_NOT_FOUND"
	ErrorInvalidNodeIdentity    ErrorCode = "INVALID_NODE_IDENTITY"
	ErrorPolicyNotFound         ErrorCode = "POLICY_NOT_FOUND"
	ErrorPolicyEvaluationFailed ErrorCode = "POLICY_EVALUATION_FAILED"
	ErrorInvalidPolicyDocument  ErrorCode = "INVALID_POLICY_DOCUMENT"
	ErrorServiceUnhealthy       ErrorCode = "SERVICE_UNHEALTHY"
	ErrorServiceOffline         ErrorCode = "SERVICE_OFFLINE"
	ErrorConnectionFailed       ErrorCode = "CONNECTION_FAILED"
	ErrorMTLSVerificationFailed ErrorCode = "MTLS_VERIFICATION_FAILED"
	ErrorInternal               ErrorCode = "INTERNAL_ERROR"
	ErrorValidation             ErrorCode = "VALIDATION_ERROR"
	ErrorUnauthorized           ErrorCode = "UNAUTHORIZED"
	ErrorNotFound               ErrorCode = "NOT_FOUND"
)

// MMAError provides detailed error information
type MMAError struct {
	Code    ErrorCode
	Message string
	Details map[string]interface{}
}

// Error implements the error interface
func (e *MMAError) Error() string {
	return e.Message
}

// NewMMAError creates a new MMAError
func NewMMAError(code ErrorCode, message string) *MMAError {
	return &MMAError{
		Code:    code,
		Message: message,
		Details: make(map[string]interface{}),
	}
}

// WithDetail adds a detail to the error
func (e *MMAError) WithDetail(key string, value interface{}) *MMAError {
	e.Details[key] = value
	return e
}

// HTTPStatus returns the HTTP status code for this error
func (e *MMAError) HTTPStatus() int {
	switch e.Code {
	case ErrorCapabilityUnknown, ErrorServiceNotFound, ErrorMemberNotFound,
		ErrorPolicyNotFound, ErrorNotFound:
		return 404
	case ErrorUnauthorized:
		return 401
	case ErrorInvalidConfig, ErrorMissingConfigRequired, ErrorValidation,
		ErrorInvalidNodeIdentity, ErrorInvalidPolicyDocument, ErrorConfigVersionMismatch:
		return 400
	case ErrorConsentRequired:
		return 403
	case ErrorRateLimited:
		return 429
	case ErrorServiceOffline, ErrorServiceUnhealthy, ErrorConnectionFailed,
		ErrorMTLSVerificationFailed:
		return 503
	default:
		return 500
	}
}
