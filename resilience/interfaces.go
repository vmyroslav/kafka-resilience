package resilience

import (
	"context"
)

//go:generate go tool moq -out moq_test.go . Producer Consumer ConsumerFactory Admin Logger StateCoordinator

// StateCoordinator abstracts the mechanism for acquiring and releasing locks on message keys.
type StateCoordinator interface {
	// Start initializes the coordinator (e.g., starts background listeners).
	Start(ctx context.Context, topic string) error

	// Acquire locks the key to ensure strict ordering.
	Acquire(ctx context.Context, originalTopic string, msg *MessageEnvelope) error

	// Release unlocks the key.
	Release(ctx context.Context, msg *MessageEnvelope) error

	// IsLocked checks if the key is currently locked.
	IsLocked(ctx context.Context, msg *MessageEnvelope) bool

	// Synchronize ensures the coordinator's local state is up-to-date with the distributed source of truth.
	// This is an OPTIONAL method. By default, the system is eventually consistent during rebalancing.
	//
	// Use Case:
	// Call this in your consumer's Setup/Rebalance handler for strict consistency. It prevents a race condition
	// where a node takes over a partition and processes a message *before* it has received all existing locks
	// from the distributed log.
	//
	// Performance Warning:
	// This method fetches the current High Water Mark for all partitions of the redirect topic (network I/O)
	// and blocks until the internal background consumer catches up. In Kafka implementations, this may
	// involve checking offsets for every partition, which can add significant latency during rebalancing.
	Synchronize(ctx context.Context) error

	// Close cleans up resources managed by the coordinator (e.g., stops background consumers).
	// The context controls the shutdown timeout - if context expires, Close returns immediately
	// with an error, potentially leaving goroutines running.
	Close(ctx context.Context) error

	// Errors returns a read-only channel of background errors from the coordinator.
	// Implementations without background goroutines may return nil.
	Errors() <-chan error
}

// Producer publishes messages to Kafka topics.
type Producer interface {
	// Produce publishes a single message to the specified topic
	Produce(ctx context.Context, topic string, msg Message) error

	// Close releases producer resources
	Close() error
}

// ConsumerHandler processes messages.
// Implementations contain the business logic for processing Kafka messages.
type ConsumerHandler interface {
	Handle(ctx context.Context, msg Message) error
}

// ConsumerHandlerFunc is a function adapter for ConsumerHandler.
type ConsumerHandlerFunc func(ctx context.Context, msg Message) error

func (f ConsumerHandlerFunc) Handle(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}

// Consumer consumes messages from Kafka topics.
type Consumer interface {
	// Consume starts consuming from the specified topics
	// Blocks until context is canceled or an error occurs
	Consume(ctx context.Context, topics []string, handler ConsumerHandler) error

	// Close stops consumption and releases resources
	Close() error
}

// ConsumerFactory creates Consumer instances.
// Used by ErrorTracker to create retry and redirect consumers.
type ConsumerFactory interface {
	// NewConsumer creates a new consumer with the specified group ID
	NewConsumer(groupID string) (Consumer, error)
}

// Logger is a minimal logging interface.
type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
}

// Admin performs Kafka cluster administration operations, to create topics and manage consumer groups.
type Admin interface {
	// CreateTopic creates a topic with specified configuration.
	// config keys: "cleanup.policy", "retention.ms", "segment.ms", etc.
	// Returns nil if topic already exists.
	CreateTopic(ctx context.Context, name string, partitions int32, replicationFactor int16, config map[string]string) error

	// DescribeTopics retrieves metadata for specified topics, skips non-existent ones.
	DescribeTopics(ctx context.Context, topics []string) ([]TopicMetadata, error)

	// DeleteConsumerGroup removes a consumer group from the cluster.
	DeleteConsumerGroup(ctx context.Context, groupID string) error

	// Close releases admin client resources.
	Close() error
}

// TopicMetadata represents topic configuration and partition information.
type TopicMetadata interface {
	// Name returns the topic name.
	Name() string

	// Partitions returns the number of partitions.
	Partitions() int32

	// PartitionOffsets returns the newest offset (High Water Mark) for each partition.
	PartitionOffsets() map[int32]int64
}
