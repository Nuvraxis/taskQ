package otelmw

import (
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"

	taskq "github.com/Nuvraxis/taskQ"
)

// Disabled reports whether tracing should be a no-op, per the standard
// OTEL_SDK_DISABLED environment variable (OpenTelemetry SDK configuration
// spec) — not a taskq-specific flag.
func Disabled() bool {
	return strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true")
}

// MiddlewareFromEnv is Middleware, gated by Disabled — a passthrough
// Middleware when tracing is off, so call sites don't need an if:
//
//	h := taskq.Chain(base, otelmw.MiddlewareFromEnv[T](tracer), taskq.RecoveryMiddleware[T]())
func MiddlewareFromEnv[T any](tracer trace.Tracer) taskq.Middleware[T] {
	if Disabled() {
		return func(next taskq.Handler[T]) taskq.Handler[T] { return next }
	}
	return Middleware[T](tracer)
}

// NewTracingBrokerFromEnv wraps next with TracingBroker, or returns next
// unwrapped when Disabled().
func NewTracingBrokerFromEnv(next taskq.Broker, tracer trace.Tracer) taskq.Broker {
	if Disabled() {
		return next
	}
	return NewTracingBroker(next, tracer)
}
