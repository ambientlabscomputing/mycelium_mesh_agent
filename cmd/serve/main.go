package serve

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/binding_engine"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/config"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/discovery"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/logging"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/policy_evaluator"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/server"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/telemetry"
	"github.com/ambientlabscomputing/mycelium_mesh_agent/internal/types"
)

// Launcher initializes and manages MMA subsystems
type Launcher struct {
	config          *config.Store
	registry        *discovery.Registry
	eventConsumer   *discovery.EventConsumer
	policyEvaluator policy_evaluator.PolicyEvaluator
	bindingEngine   binding_engine.BindingEngine
	telemetryBuffer *telemetry.Buffer
	collector       *telemetry.Collector
	flusher         *telemetry.Flusher
	apiServer       *server.Server
}

// LauncherConfig specifies launcher configuration
type LauncherConfig struct {
	LocalNodeID string
	HTTPPort    int
	LogLevel    string
	ConfigStore *config.Store
}

// NewLauncher creates a new launcher
func NewLauncher(cfg LauncherConfig) *Launcher {
	return &Launcher{
		config: cfg.ConfigStore,
	}
}

// Start initializes all subsystems
func (l *Launcher) Start(ctx context.Context) error {
	logger := logging.GetLogger(ctx)
	logger.Info("launcher starting")

	// Get node ID from config or environment
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "local-node"
	}

	// Initialize registry
	l.registry = discovery.NewRegistry()
	logger.Info("registry initialized")

	// Initialize discovery
	discoveryConfig := l.config.GetDiscoveryConfig()
	l.eventConsumer = discovery.NewEventConsumer(l.registry, discovery.UAEventConsumerConfig{
		StreamEndpoint:    discoveryConfig.MDNSServiceName,
		ReconnectInterval: 5 * time.Second,
		BufferSize:        1000,
	})

	if err := l.eventConsumer.Start(ctx); err != nil {
		logger.Error("failed to start event consumer", "error", err)
		return err
	}
	logger.Info("event consumer started")

	// Initialize capability cache (for now, empty)
	capCache := &types.CapabilityCache{
		Version:           "1.0.0",
		Capabilities:      make(map[string]*types.Capability),
		SchemaIndexDigest: "",
		VerifiedAt:        time.Now(),
	}

	// Initialize policy evaluator
	l.policyEvaluator = policy_evaluator.NewEvaluator(logger, capCache)
	logger.Info("policy evaluator initialized")

	// Initialize binding engine
	l.bindingEngine = binding_engine.NewEngine(logger, l.registry, l.policyEvaluator)

	if err := l.bindingEngine.Start(ctx); err != nil {
		logger.Error("failed to start binding engine", "error", err)
		return err
	}
	logger.Info("binding engine started")

	// Initialize telemetry
	telemetryConfig := l.config.GetTelemetryConfig()
	l.telemetryBuffer = telemetry.NewBuffer(telemetryConfig.BufferSizeBytes, telemetryConfig.FlushIntervalMs)
	l.collector = telemetry.NewCollector(telemetryConfig.FlushIntervalMs)
	l.flusher = telemetry.NewFlusher(l.telemetryBuffer, l.collector)

	if err := l.telemetryBuffer.Start(ctx); err != nil {
		logger.Error("failed to start telemetry buffer", "error", err)
		return err
	}
	logger.Info("telemetry initialized")

	// Initialize API server
	apiServer := server.NewServer(
		logger,
		l.bindingEngine,
		l.registry,
		l.flusher,
		l.config,
		8080,
	)

	if err := apiServer.Start(ctx); err != nil {
		logger.Error("failed to start API server", "error", err)
		return err
	}
	l.apiServer = apiServer
	logger.Info("API server started")

	logger.Info("launcher complete - all subsystems started")
	return nil
}

// Stop shuts down all subsystems gracefully
func (l *Launcher) Stop(ctx context.Context) error {
	logger := logging.GetLogger(ctx)
	logger.Info("launcher stopping")

	// Stop in reverse order of initialization

	if l.apiServer != nil {
		if err := l.apiServer.Stop(ctx); err != nil {
			logger.Error("failed to stop API server", "error", err)
		}
	}

	if l.bindingEngine != nil {
		if err := l.bindingEngine.Stop(ctx); err != nil {
			logger.Error("failed to stop binding engine", "error", err)
		}
	}

	if l.telemetryBuffer != nil {
		if err := l.telemetryBuffer.Stop(ctx); err != nil {
			logger.Error("failed to stop telemetry buffer", "error", err)
		}
	}

	if l.eventConsumer != nil {
		if err := l.eventConsumer.Stop(ctx); err != nil {
			logger.Error("failed to stop event consumer", "error", err)
		}
	}

	logger.Info("launcher stopped")
	return nil
}

// ===== Runtime =====

// Runtime manages the MMA process lifecycle
type Runtime struct {
	launcher *Launcher
	signals  chan os.Signal
}

// NewRuntime creates a new runtime
func NewRuntime(launcher *Launcher) *Runtime {
	return &Runtime{
		launcher: launcher,
		signals:  make(chan os.Signal, 1),
	}
}

// Run starts the runtime and manages graceful shutdown
func (r *Runtime) Run(ctx context.Context) error {
	logger := logging.GetLogger(ctx)
	logger.Info("runtime starting")

	// Register signal handlers
	signal.Notify(r.signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Create cancellable context
	runtimeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start launcher
	errChan := make(chan error, 1)
	go func() {
		errChan <- r.launcher.Start(runtimeCtx)
	}()

	// Wait for signal or error
	select {
	case sig := <-r.signals:
		logger.Info("received signal", "signal", sig)

		// Handle signal
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("initiating graceful shutdown")
			cancel()

			// Give graceful shutdown 30 seconds
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()

			if err := r.launcher.Stop(shutdownCtx); err != nil {
				logger.Error("error during shutdown", "error", err)
				return err
			}

		case syscall.SIGHUP:
			logger.Info("received SIGHUP - reloading config")
			// TODO: Implement config reload

		default:
			logger.Warn("received unexpected signal", "signal", sig)
		}

	case err := <-errChan:
		if err != nil {
			logger.Error("launcher error", "error", err)
			return err
		}
	}

	logger.Info("runtime stopped")
	return nil
}

// Serve is the main entry point for the serve command
func Serve() error {
	// Initialize logging
	logFile := "mma.log"
	ctx, logger := logging.Init(context.Background(), logging.LoggerModeAgent, &logFile)
	defer logger.Info("MMA shutdown complete")

	logger.Info("Mycelium Mesh Agent starting")

	// Load configuration
	cfgStore := config.NewStore()
	cfg := cfgStore.Get()

	logger.Info("configuration loaded",
		"mesh_mode", cfg.MeshMode,
		"locality_defaults", cfg.LocalityDefaults,
		"control_channel", cfg.ControlChannelConfig.Transport,
	)

	// Create launcher
	launcher := NewLauncher(LauncherConfig{
		LocalNodeID: os.Getenv("NODE_ID"),
		HTTPPort:    8080,
		LogLevel:    cfg.TelemetryDefaults.Level,
		ConfigStore: cfgStore,
	})

	// Create and run runtime
	runtime := NewRuntime(launcher)
	return runtime.Run(ctx)
}

// func main() would be in the actual main.go file
