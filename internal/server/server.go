package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/binding_engine"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/telemetry"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
	"github.com/gin-gonic/gin"
)

// Server is the HTTP API server.
type Server struct {
	router        *gin.Engine
	log           *slog.Logger
	bindingEngine binding_engine.BindingEngine
	registry      *discovery.Registry
	flusher       *telemetry.Flusher
	configStore   *config.Store
	httpServer    *http.Server
}

// NewServer creates a new HTTP API server.
func NewServer(
	log *slog.Logger,
	bindingEngine binding_engine.BindingEngine,
	registry *discovery.Registry,
	flusher *telemetry.Flusher,
	configStore *config.Store,
	port int,
) *Server {
	router := gin.Default()

	s := &Server{
		router:        router,
		log:           log,
		bindingEngine: bindingEngine,
		registry:      registry,
		flusher:       flusher,
		configStore:   configStore,
	}

	// Setup routes
	s.setupRoutes()

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	return s
}

// setupRoutes registers all API endpoints.
func (s *Server) setupRoutes() {
	// Enforce maximum request body size
	s.router.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxRequestBodySize)
		c.Next()
	})

	// Health check
	s.router.GET("/health", s.handleHealth)

	// API v1 routes
	api := s.router.Group("/api/v1")

	// Binding endpoints
	api.POST("/bindings/request", s.handleBindingRequest)
	api.GET("/bindings/:binding_id", s.handleGetBinding)

	// Introspection endpoints
	api.POST("/introspect/service", s.handleIntrospectService)
	api.GET("/introspect/mesh", s.handleIntrospectMesh)

	// Telemetry endpoints
	api.POST("/telemetry/flush", s.handleTelemetryFlush)

	// Configuration endpoints
	api.POST("/config/apply", s.handleConfigApply)

	// Metrics endpoint
	s.router.GET("/metrics", s.handleMetrics)
}

// handleHealth returns server health status.
func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

// handleBindingRequest handles POST /api/v1/bindings/request.
func (s *Server) handleBindingRequest(c *gin.Context) {
	var req types.BindingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.log.Error("Invalid binding request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request format"})
		return
	}

	// Validate required fields
	if req.Client.ServiceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client.service_id is required"})
		return
	}
	if req.Capability.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "capability.id is required"})
		return
	}

	// Validate field lengths
	if len(req.Client.ServiceID) > MaxServiceIDLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client.service_id exceeds maximum length"})
		return
	}
	if len(req.Capability.ID) > MaxCapabilityIDLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "capability.id exceeds maximum length"})
		return
	}
	if len(req.RequestID) > MaxRequestIDLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request_id exceeds maximum length"})
		return
	}

	// Validate locality preference if provided
	if req.Constraints.Locality != "" {
		validLocalities := []types.LocalityPreference{
			types.LocalityNodeOnly,
			types.LocalityLANPreferred,
			types.LocalityLANOnly,
			types.LocalityWANAllowed,
		}
		valid := false
		for _, loc := range validLocalities {
			if req.Constraints.Locality == loc {
				valid = true
				break
			}
		}
		if !valid {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid locality preference"})
			return
		}
	}

	resp := s.bindingEngine.RequestBinding(c.Request.Context(), &req)
	c.JSON(http.StatusOK, resp)
}

// handleGetBinding handles GET /api/v1/bindings/{binding_id}.
func (s *Server) handleGetBinding(c *gin.Context) {
	bindingID := c.Param("binding_id")
	if bindingID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "binding_id required"})
		return
	}

	// Validate binding ID format and length
	if len(bindingID) > MaxBindingIDLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "binding_id exceeds maximum length"})
		return
	}

	status, err := s.bindingEngine.GetBinding(c.Request.Context(), bindingID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "binding not found"})
		return
	}

	c.JSON(http.StatusOK, status)
}

// handleIntrospectService handles POST /api/v1/introspect/service.
func (s *Server) handleIntrospectService(c *gin.Context) {
	var req struct {
		ServiceID string `json:"service_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		s.log.Error("Invalid introspect request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request format"})
		return
	}

	// Validate service ID
	if req.ServiceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service_id is required"})
		return
	}
	if len(req.ServiceID) > MaxServiceIDLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service_id exceeds maximum length"})
		return
	}

	service := s.registry.GetService(req.ServiceID)
	if service == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "service not found"})
		return
	}

	c.JSON(http.StatusOK, service)
}

// handleIntrospectMesh handles GET /api/v1/introspect/mesh.
func (s *Server) handleIntrospectMesh(c *gin.Context) {
	members := s.registry.ListMembers()
	services := s.registry.ListServices()
	capabilities := s.registry.ListCapabilities()

	resp := gin.H{
		"members":          members,
		"services":         services,
		"capabilities":     capabilities,
		"member_count":     len(members),
		"service_count":    len(services),
		"capability_count": len(capabilities),
	}

	c.JSON(http.StatusOK, resp)
}

// handleTelemetryFlush handles POST /api/v1/telemetry/flush.
func (s *Server) handleTelemetryFlush(c *gin.Context) {
	var req struct {
		Types []string `json:"types"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		s.log.Error("Invalid telemetry flush request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request format"})
		return
	}

	// Validate types array
	if len(req.Types) > MaxTypesArrayLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": "types array exceeds maximum length"})
		return
	}
	for _, t := range req.Types {
		if len(t) > MaxTypeNameLength {
			c.JSON(http.StatusBadRequest, gin.H{"error": "type name exceeds maximum length"})
			return
		}
	}

	flushReq := &types.TelemetryFlushRequest{
		Types: req.Types,
	}

	resp, err := s.flusher.FlushRequest(c.Request.Context(), flushReq)
	if err != nil {
		s.log.Error("Telemetry flush error", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// handleConfigApply handles POST /api/v1/config/apply.
func (s *Server) handleConfigApply(c *gin.Context) {
	var cfg config.MMAConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		s.log.Error("Invalid config request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid config format"})
		return
	}

	// Validate required config fields
	if cfg.ConfigVersion == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "config_version is required"})
		return
	}
	if cfg.MeshMode == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "mesh_mode is required"})
		return
	}

	// Validate mesh mode
	validModes := []string{"sidecar", "ambient", "sdk"}
	validMode := false
	for _, mode := range validModes {
		if cfg.MeshMode == mode {
			validMode = true
			break
		}
	}
	if !validMode {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid mesh_mode (must be sidecar, ambient, or sdk)"})
		return
	}

	if err := s.configStore.ValidateConfig(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.configStore.Set(&cfg)
	s.log.Info("Config applied")

	c.JSON(http.StatusOK, gin.H{
		"status": "applied",
	})
}

// handleMetrics handles GET /metrics.
func (s *Server) handleMetrics(c *gin.Context) {
	metrics := s.flusher.GetMetrics(c.Request.Context())
	c.JSON(http.StatusOK, metrics)
}

// Start starts the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	s.log.Info("Starting HTTP server", "addr", s.httpServer.Addr)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("Server error", "error", err)
		}
	}()
	return nil
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}
