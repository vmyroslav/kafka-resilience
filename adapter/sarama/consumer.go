package sarama

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// ConsumerAdapter wraps sarama.ConsumerGroup to implement retry.Consumer interface.
type ConsumerAdapter struct {
	consumerGroup sarama.ConsumerGroup
}

// NewConsumerAdapter creates a retry.Consumer from a Sarama ConsumerGroup.
func NewConsumerAdapter(cg sarama.ConsumerGroup) resilience.Consumer {
	return &ConsumerAdapter{consumerGroup: cg}
}

// Consume implements retry.Consumer interface.
func (c *ConsumerAdapter) Consume(ctx context.Context, topics []string, handler resilience.ConsumerHandler) error {
	saramaHandler := &consumerGroupHandler{
		retryHandler: handler,
	}

	// start consuming, the loop is required because Sarama's Consume method returns nil
	// when a rebalance occurs, necessitating a restart to rejoin the consumer group.
	for {
		if err := c.consumerGroup.Consume(ctx, topics, saramaHandler); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Close implements retry.Consumer interface.
func (c *ConsumerAdapter) Close() error {
	return c.consumerGroup.Close()
}

// consumerGroupHandler adapts retry.ConsumerHandler to sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	retryHandler resilience.ConsumerHandler
}

// Setup implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			retryMsg := NewMessage(msg)

			if err := h.retryHandler.Handle(session.Context(), retryMsg); err != nil {
				return err
			}

			// mark message as processed
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// ConsumerFactory implements retry.ConsumerFactory interface for Sarama.
// This factory creates new Sarama consumer groups with different group IDs.
type ConsumerFactory struct {
	client sarama.Client
}

// NewConsumerFactory creates a retry.ConsumerFactory from a Sarama Client.
func NewConsumerFactory(client sarama.Client) resilience.ConsumerFactory {
	return &ConsumerFactory{client: client}
}

// NewConsumer implements retry.ConsumerFactory interface.
func (f *ConsumerFactory) NewConsumer(groupID string) (resilience.Consumer, error) {
	cg, err := sarama.NewConsumerGroupFromClient(groupID, f.client)
	if err != nil {
		return nil, err
	}

	return NewConsumerAdapter(cg), nil
}
