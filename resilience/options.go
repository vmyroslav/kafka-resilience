package resilience

import "go.opentelemetry.io/otel/metric"

// TrackerOption is a functional option for configuring the ErrorTracker.
type TrackerOption func(*trackerOptions)

type trackerOptions struct {
	logger      Logger
	coordinator StateCoordinator
	backoff     BackoffStrategy
	meter       metric.Meter
}

// WithLogger sets the logger. Defaults to NoOpLogger if not set.
func WithLogger(l Logger) TrackerOption {
	return func(o *trackerOptions) {
		o.logger = l
	}
}

// WithCoordinator sets a custom state coordinator.
// Defaults to KafkaStateCoordinator if not set.
func WithCoordinator(c StateCoordinator) TrackerOption {
	return func(o *trackerOptions) {
		o.coordinator = c
	}
}

// WithBackoff sets a custom backoff strategy. Defaults to ExponentialBackoff if not set.
func WithBackoff(b BackoffStrategy) TrackerOption {
	return func(o *trackerOptions) {
		o.backoff = b
	}
}

// WithMeter sets the OpenTelemetry meter for metrics instrumentation.
// If not set, the global meter provider is used (which is a no-op if no OTel SDK is registered).
func WithMeter(m metric.Meter) TrackerOption {
	return func(o *trackerOptions) {
		o.meter = m
	}
}
