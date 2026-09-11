package logger

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LogLevel represents the level for the log.
type LogLevel string

const (
	// ProductionLogLevel is the level for the production log.
	ProductionLogLevel LogLevel = "PRODUCTION"
	// DevelopmentLogLevel is the level for development log.
	DevelopmentLogLevel LogLevel = "DEVELOPMENT"
	// EnvLoggerLevel is the environment variable name for log level.
	EnvLoggerLevel = "LOGGER_LEVEL"
	// LogCtxIDKey holds the TraceId for log.
	LogCtxIDKey = "TraceId"
)

var (
	defaultLogLevel LogLevel
	logger          *zap.Logger
	loggerMutex     sync.Mutex
)

// loggerKey holds the context key used for loggers.
type loggerKey struct{}

// SetLoggerLevel helps set defaultLogLevel, using which newLogger func helps
// create either development logger or production logger
func SetLoggerLevel(logLevel LogLevel) {
	defaultLogLevel = logLevel
	if logLevel != ProductionLogLevel && logLevel != DevelopmentLogLevel {
		defaultLogLevel = ProductionLogLevel
	}
	GetLoggerWithNoContext().Infof("Setting default log level to :%q", defaultLogLevel)
}

// getLogger returns the logger associated with the given context.
// If there is no logger associated with context, getLogger func will return
// a new logger.
func getLogger(ctx context.Context) *zap.Logger {
	if logger, _ := ctx.Value(loggerKey{}).(*zap.Logger); logger != nil {
		return logger
	}
	return newLogger()
}

// GetLogger returns SugaredLogger associated with given context.
func GetLogger(ctx context.Context) *zap.SugaredLogger {
	return getLogger(ctx).Sugar()
}

// NewContextWithLogger returns a new child context with context UUID set
// using key CtxId.
func NewContextWithLogger(ctx context.Context) context.Context {
	newCtx := withFields(ctx, zap.String(LogCtxIDKey, uuid.New().String()))
	return newCtx
}

// GetNewContextWithLogger creates a new context with context UUID and logger
// set func returns both context and logger to the caller.
func GetNewContextWithLogger() (context.Context, *zap.SugaredLogger) {
	newCtx := NewContextWithLogger(context.Background())
	return newCtx, GetLogger(newCtx)
}

// withFields returns a new context derived from ctx
// that has a logger that always logs the given fields.
func withFields(ctx context.Context, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, loggerKey{}, getLogger(ctx).With(fields...))
}

// newLogger creates and returns a logger according to the logLevel set.
// Uses singleton pattern to create only one instance of logger.
func newLogger() *zap.Logger {
	if logger != nil {
		// logger is already initialized
		return logger
	}

	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	// this check ensures that multiple threads don't override the logger
	// created by the first thread acquiring the lock.
	if logger != nil {
		return logger
	}

	if defaultLogLevel == DevelopmentLogLevel {
		logger, _ = zap.NewDevelopment()
	} else {
		loggerConfig := zap.NewProductionConfig()
		loggerConfig.EncoderConfig.TimeKey = "time"
		loggerConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
		logger, _ = loggerConfig.Build()
	}

	return logger
}

// GetLoggerWithNoContext returns a new logger to the caller.
// Returned logger is not associated with any context.
func GetLoggerWithNoContext() *zap.SugaredLogger {
	return newLogger().Sugar()
}

// LogNewError logs an error msg, and returns error with msg.
func LogNewError(log *zap.SugaredLogger, msg string) error {
	log.Desugar().WithOptions(zap.AddCallerSkip(1)).Sugar().Error(msg)
	return errors.New(msg)
}

// LogNewErrorf logs a formated msg, and returns error with msg.
func LogNewErrorf(log *zap.SugaredLogger, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	log.Desugar().WithOptions(zap.AddCallerSkip(1)).Sugar().Error(msg)
	return errors.New(msg)
}

// LogNewErrorCode logs an error msg, and returns error with code and msg.
func LogNewErrorCode(log *zap.SugaredLogger, c codes.Code, msg string) error {
	log.Desugar().WithOptions(zap.AddCallerSkip(1)).Sugar().Error(msg)
	return status.Error(c, msg)
}

// LogNewErrorCodef logs a formated msg, and returns error with code and msg.
func LogNewErrorCodef(log *zap.SugaredLogger, c codes.Code, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	log.Desugar().WithOptions(zap.AddCallerSkip(1)).Sugar().Error(msg)
	return status.Error(c, msg)
}

// secretsGetter is implemented by every generated CSI request type that
// carries a "Secrets" field (e.g. NodeStageVolumeRequest, CreateVolumeRequest).
type secretsGetter interface {
	GetSecrets() map[string]string
}

// RedactCSIRequest returns a value suitable for logging a CSI RPC request
// with %+v: if the request carries a Secrets map, it is replaced with a
// fixed-size redaction marker so credentials never reach the log stream.
// Non-secret-bearing request types are returned unchanged.
func RedactCSIRequest(req interface{}) interface{} {
	sg, ok := req.(secretsGetter)
	if !ok || len(sg.GetSecrets()) == 0 {
		return req
	}

	// Copy the request so the caller's original object is left untouched,
	// then zero out the Secrets field on the copy via reflection since the
	// concrete type is only known through the secretsGetter interface.
	v := reflect.ValueOf(req)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return req
	}
	cp := reflect.New(v.Elem().Type())
	cp.Elem().Set(v.Elem())
	if f := cp.Elem().FieldByName("Secrets"); f.IsValid() && f.CanSet() {
		redacted := reflect.MakeMapWithSize(f.Type(), 1)
		redacted.SetMapIndex(reflect.ValueOf("***stripped***"), reflect.ValueOf("***stripped***"))
		f.Set(redacted)
	}
	return cp.Interface()
}
