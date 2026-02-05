package logging

import (
	"context"
	"log/slog"
	"os"
	"runtime"

	"github.com/lmittmann/tint"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger *slog.Logger

type LogKey struct{}

const (
	LoggerModeCLI   = "cli"   // logs to file in json format, uses lumberjack for log rotation
	LoggerModeAgent = "agent" // logs to file in json format, uses lumberjack for log rotation
	LoggerModeDev   = "dev"   // logs to stderr in human friendly tint format
)

func Init(parentCtx context.Context, mode string, filePath *string) (context.Context, *slog.Logger) {
	var handler slog.Handler
	switch mode {
	case LoggerModeCLI, LoggerModeAgent:
		var logFile string
		if filePath != nil {
			logFile = *filePath
		} else if mode == LoggerModeCLI {
			logFile = "underleaf_cli.log"
		} else {
			logFile = "underleaf_agent.log"
		}
		handler = slog.NewJSONHandler(&lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    10, // megabytes
			MaxBackups: 5,
			MaxAge:     28,   //days
			Compress:   true, // disabled by default
		}, &slog.HandlerOptions{Level: slog.LevelDebug})
	case LoggerModeDev:
		handler = tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug})
	default:
		panic("unknown logger mode: " + mode)
	}

	logger = slog.New(handler)
	logger = AddRuntimeValues(logger)
	slog.SetDefault(logger)
	ctx := context.WithValue(parentCtx, LogKey{}, logger)
	return ctx, logger
}

func AddRuntimeValues(l *slog.Logger) *slog.Logger {
	os := runtime.GOOS
	arch := runtime.GOARCH
	return l.With("os", os, "arch", arch)
}

func AddRuntimeValuesToCtx(ctx context.Context) context.Context {
	logger := GetLogger(ctx)
	logger = AddRuntimeValues(logger)
	return context.WithValue(ctx, LogKey{}, logger)
}

func ContextualizeLogger(ctx context.Context, args ...any) context.Context {
	if loggerFromCtx, ok := ctx.Value(LogKey{}).(*slog.Logger); ok {
		loggerFromCtx = loggerFromCtx.With(args...)
		return context.WithValue(ctx, LogKey{}, loggerFromCtx)
	}
	ctx = context.WithValue(ctx, LogKey{}, logger.With(args...))
	return ctx
}

func GetLogger(ctx context.Context) *slog.Logger {
	if loggerFromCtx, ok := ctx.Value(LogKey{}).(*slog.Logger); ok {
		return loggerFromCtx
	}
	return logger
}

func GetCtxWithLogger(ctx context.Context) (context.Context, *slog.Logger) {
	if loggerFromCtx, ok := ctx.Value(LogKey{}).(*slog.Logger); ok {
		return ctx, loggerFromCtx
	}
	return ctx, logger
}
