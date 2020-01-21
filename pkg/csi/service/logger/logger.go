package logger

import (
	"context"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LogLevel string

const (
	ProductionLogLevel  LogLevel = "PRODUCTION"
	DevelopmentLogLevel LogLevel = "DEVELOPMENT"
	EnvLoggerLevel               = "LOGGER_LEVEL"
	LogCtxIdKey                  = "TraceId"
)

var defaultLogLevel LogLevel

// loggerKey holds the context key used for loggers.
type loggerKey struct{}

// SetLoggerLevel helps set defaultLogLevel, using which newLogger func helps
// create either development logger or production logger
func SetLoggerLevel(logLevel LogLevel) {
	defaultLogLevel = logLevel
	if logLevel != ProductionLogLevel && logLevel != DevelopmentLogLevel {
		defaultLogLevel = ProductionLogLevel
	}
	newLogger().Sugar().Infof("Setting default log level to :%q", defaultLogLevel)
}

// getLogger returns the logger associated with the given context.
// If there is no logger associated with context, getLogger func will return a new logger.
func getLogger(ctx context.Context) *zap.Logger {
	if logger, _ := ctx.Value(loggerKey{}).(*zap.Logger); logger != nil {
		return logger
	}
	return newLogger()
}

// GetLogger returns SugaredLogger associated with given context.
// this func is using private func getLogger
func GetLogger(ctx context.Context) *zap.SugaredLogger {
	return getLogger(ctx).Sugar()
}

// NewContextWithLogger returns a new child context with context UUID set using key CtxId
func NewContextWithLogger(ctx context.Context) context.Context {
	newCtx := withFields(ctx, zap.String(LogCtxIdKey, uuid.New().String()))
	return newCtx
}

// GetNewContextWithLogger creates a new context with context UUID and logger set
// func returns both context and logger to the caller.
func GetNewContextWithLogger() (context.Context, *zap.SugaredLogger) {
	newCtx := withFields(context.Background(), zap.String(LogCtxIdKey, uuid.New().String()))
	return newCtx, GetLogger(newCtx)
}

// withLogger returns a new context derived from ctx that
// is associated with the given logger.
func withLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// withFields returns a new context derived from ctx
// that has a logger that always logs the given fields.
func withFields(ctx context.Context, fields ...zapcore.Field) context.Context {
	return withLogger(ctx, getLogger(ctx).With(fields...))
}

// newLogger creates and return a new logger depending logLevel set
func newLogger() *zap.Logger {
	var logger *zap.Logger
	if defaultLogLevel == DevelopmentLogLevel {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}
	return logger
}

// GetLoggerWithNoContext returns a new logger to the caller.
// returned logger is not associated with any context
func GetLoggerWithNoContext() *zap.SugaredLogger {
	return newLogger().Sugar()
}
