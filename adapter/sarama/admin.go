package sarama

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// AdminAdapter wraps sarama.ClusterAdmin to implement retry.Admin interface.
type AdminAdapter struct {
	admin  sarama.ClusterAdmin
	client sarama.Client
}

// NewAdminAdapter creates a retry.Admin from a Sarama Client.
func NewAdminAdapter(client sarama.Client) (resilience.Admin, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}

	return &AdminAdapter{
		admin:  admin,
		client: client,
	}, nil
}

// CreateTopic creates a topic with the given specifications.
func (a *AdminAdapter) CreateTopic(
	_ context.Context,
	name string,
	partitions int32,
	replicationFactor int16,
	config map[string]string,
) error {
	// convert config map[string]string to map[string]*string (Sarama format)
	saramaConfig := make(map[string]*string, len(config))
	for k, v := range config {
		val := v
		saramaConfig[k] = &val
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     saramaConfig,
	}

	err := a.admin.CreateTopic(name, topicDetail, false)
	if err != nil {
		// ignore "topic already exists" error
		var topicErr *sarama.TopicError
		if errors.As(err, &topicErr) {
			if errors.Is(topicErr.Err, sarama.ErrTopicAlreadyExists) {
				return nil
			}
		}

		return fmt.Errorf("failed to create topic %s: %w", name, err)
	}

	return nil
}

// DescribeTopics retrieves metadata for the specified topics.
func (a *AdminAdapter) DescribeTopics(_ context.Context, topics []string) ([]resilience.TopicMetadata, error) {
	// get metadata
	metadata, err := a.admin.DescribeTopics(topics)
	if err != nil {
		return nil, err
	}

	result := make([]resilience.TopicMetadata, 0, len(metadata))
	for _, md := range metadata {
		if !errors.Is(md.Err, sarama.ErrNoError) {
			continue
		}

		// get offsets
		offsets := make(map[int32]int64)
		for _, p := range md.Partitions {
			off, err := a.client.GetOffset(md.Name, p.ID, sarama.OffsetNewest)
			if err != nil {
				return nil, fmt.Errorf("failed to get offset for topic %s partition %d: %w", md.Name, p.ID, err)
			}
			offsets[p.ID] = off
		}

		result = append(result, &topicMetadata{
			name:       md.Name,
			partitions: int32(min(len(md.Partitions), math.MaxInt32)),
			offsets:    offsets,
		})
	}

	return result, nil
}

// DeleteConsumerGroup deletes the specified consumer group.
func (a *AdminAdapter) DeleteConsumerGroup(_ context.Context, groupID string) error {
	return a.admin.DeleteConsumerGroup(groupID)
}

// Close closes the admin connection.
func (a *AdminAdapter) Close() error {
	return a.admin.Close()
}

// topicMetadata implements retry.TopicMetadata interface.
type topicMetadata struct {
	name       string
	partitions int32
	offsets    map[int32]int64
}

func (t *topicMetadata) Name() string                      { return t.name }
func (t *topicMetadata) Partitions() int32                 { return t.partitions }
func (t *topicMetadata) PartitionOffsets() map[int32]int64 { return t.offsets }
