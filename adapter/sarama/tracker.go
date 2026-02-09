package sarama

import (
	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type managerOptions struct {
	logger      resilience.Logger
	coordinator resilience.StateCoordinator
	backoff     resilience.BackoffStrategy
	meter       metric.Meter
}

// NewResilienceTracker creates a configured ErrorTracker using the Sarama client.
// It handles the creation of all necessary adapters and internal components.
func NewResilienceTracker(cfg *resilience.Config, client sarama.Client, opts ...ManagerOption) (*resilience.ErrorTracker, error) {
	options := &managerOptions{
		logger: resilience.NewNoOpLogger(),
	}
	for _, opt := range opts {
		opt(options)
	}

	// note: we don't close producer, because it shares the underlying client
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	producerAdapter := NewProducerAdapter(saramaProducer)
	consumerFactory := NewConsumerFactory(client)
	adminAdapter, err := NewAdminAdapter(client)
	if err != nil {
		return nil, err
	}

	coordinator := options.coordinator
	if coordinator == nil {
		coordinator = resilience.NewKafkaStateCoordinator(
			cfg,
			options.logger,
			producerAdapter,
			consumerFactory,
			adminAdapter,
			options.meter,
		)
	}

	backoff := options.backoff
	if backoff == nil {
		backoff = resilience.NewExponentialBackoff()
	}

	return resilience.NewErrorTracker(
		cfg,
		options.logger,
		producerAdapter,
		consumerFactory,
		adminAdapter,
		coordinator,
		backoff,
		options.meter,
	)
}

// ManagerOption is a functional option for configuring the resilience manager.
type ManagerOption func(*managerOptions)

// WithLogger sets the logger for the manager.
func WithLogger(logger resilience.Logger) ManagerOption {
	return func(o *managerOptions) {
		o.logger = logger
	}
}

// WithCoordinator sets a custom state coordinator.
func WithCoordinator(coordinator resilience.StateCoordinator) ManagerOption {
	return func(o *managerOptions) {
		o.coordinator = coordinator
	}
}

// WithBackoff sets a custom backoff strategy.
func WithBackoff(backoff resilience.BackoffStrategy) ManagerOption {
	return func(o *managerOptions) {
		o.backoff = backoff
	}
}

// WithMeter sets the OpenTelemetry meter for metrics instrumentation.
// If not set, the global meter provider is used (which is a no-op if no OTel SDK is registered).
func WithMeter(meter metric.Meter) ManagerOption {
	return func(o *managerOptions) {
		o.meter = meter
	}
}

// WithNoMetrics explicitly disables metrics, even if a global OTel SDK is registered.
func WithNoMetrics() ManagerOption {
	return func(o *managerOptions) {
		o.meter = noop.Meter{}
	}
}
