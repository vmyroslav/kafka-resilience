package sarama

import (
	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// NewResilienceTracker creates a configured ErrorTracker using the Sarama client.
// It handles the creation of all necessary adapters (producer, consumer factory, admin)
// from the provided Sarama client. Use resilience.TrackerOption to configure optional
// dependencies like logger, backoff strategy, coordinator, and metrics.
func NewResilienceTracker(cfg *resilience.Config, client sarama.Client, opts ...resilience.TrackerOption) (*resilience.ErrorTracker, error) {
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

	return resilience.NewErrorTracker(
		cfg,
		producerAdapter,
		consumerFactory,
		adminAdapter,
		opts...,
	)
}
