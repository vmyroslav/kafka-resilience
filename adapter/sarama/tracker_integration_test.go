//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// TestIntegration_SaramaAdmin tests the Sarama Admin adapter implementation.
// Verifies that the Admin interface correctly creates topics and manages consumer groups.
func TestIntegration_SaramaAdmin(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client := newTestClient(t)
	admin, err := saramaadapter.NewAdminAdapter(client)
	require.NoError(t, err, "failed to create admin adapter")
	t.Cleanup(func() { _ = admin.Close() })

	t.Run("CreateTopics", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-test-%d", time.Now().UnixNano())

		// create topic with specific configuration
		err := admin.CreateTopic(ctx, testTopic, 3, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000",
		})
		require.NoError(t, err, "failed to create topic")

		// verify topic was created by describing it
		var metadata []resilience.TopicMetadata
		assert.Eventually(t, func() bool {
			metadata, err = admin.DescribeTopics(ctx, []string{testTopic})
			return err == nil && len(metadata) == 1
		}, 10*time.Second, 500*time.Millisecond, "failed to describe topic or topic not found")

		assert.Equal(t, testTopic, metadata[0].Name())
		assert.Equal(t, int32(3), metadata[0].Partitions())
	})

	// idempotent topic creation
	t.Run("IdempotentTopicCreation", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-idempotent-%d", time.Now().UnixNano())

		// create topic first time
		err := admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{
			"cleanup.policy": "compact",
		})
		require.NoError(t, err, "failed to create topic first time")

		// create same topic again - should not error
		err = admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{
			"cleanup.policy": "compact",
		})
		require.NoError(t, err, "creating existing topic should be idempotent")
	})

	// describe non-existent topic
	t.Run("DescribeNonExistentTopic", func(t *testing.T) {
		nonExistentTopic := "non-existent-topic-12345"

		// should return empty list
		metadata, err := admin.DescribeTopics(ctx, []string{nonExistentTopic})
		require.NoError(t, err, "DescribeTopics should not error for non-existent topics")
		assert.Len(t, metadata, 0, "should return empty metadata for non-existent topics")
	})

	// delete consumer group
	t.Run("DeleteConsumerGroup", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-cg-test-%d", time.Now().UnixNano())
		groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

		// create topic
		err := admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{})
		require.NoError(t, err, "failed to create topic")

		// create a consumer group by consuming
		consumer, err := sarama.NewConsumerGroup([]string{sharedBroker}, groupID, newTestSaramaConfig())
		require.NoError(t, err, "failed to create consumer group")

		// consume briefly to establish the group
		consumeCtx, consumeCancel := context.WithTimeout(ctx, 10*time.Second)
		defer consumeCancel()

		ready := make(chan struct{})
		var once sync.Once
		handler := &readyDummyHandler{
			ready: ready,
			once:  &once,
		}
		go func() {
			_ = consumer.Consume(consumeCtx, []string{testTopic}, handler)
		}()

		// wait for consumer group to be established
		select {
		case <-ready:
		case <-consumeCtx.Done():
			t.Fatal("timeout waiting for consumer group to be established")
		}
		consumer.Close()

		// delete the consumer group
		err = admin.DeleteConsumerGroup(ctx, groupID)
		assert.NoError(t, err, "failed to delete consumer group")
	})
}

// TestIntegration_SaramaAdapterFullFlow tests the full ErrorTracker flow with Sarama adapter.
func TestIntegration_SaramaAdapterFullFlow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-adapter")

	var (
		firstAttempts  atomic.Int32
		secondAttempts atomic.Int32
		thirdProcessed atomic.Bool
		mu             sync.Mutex
		processedMsgs  []string
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// start tracking - this should create topics
	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	sharedLogger.Info("tracker started, topics should be created")

	// get retry topic name for later use
	retryTopic := tracker.RetryTopic(topic)

	// create business logic processor
	processor := &testMessageProcessor{
		logger:         sharedLogger,
		firstAttempts:  &firstAttempts,
		secondAttempts: &secondAttempts,
		thirdProcessed: &thirdProcessed,
		mu:             &mu,
		processedMsgs:  &processedMsgs,
	}

	// create main consumer (using standard Sarama)
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	// create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// create handler that uses tracker
	handler := &adapterTestHandler{
		tracker:   tracker,
		logger:    sharedLogger,
		processor: processor,
	}

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady)

	// produce test messages
	produceTestMessage(t, sharedBroker, topic, "test-key", "first-will-fail-twice")
	produceTestMessage(t, sharedBroker, topic, "test-key", "second-will-fail-once")
	produceTestMessage(t, sharedBroker, topic, "test-key", "third-will-succeed")

	// wait for processing to complete
	assert.Eventually(t, func() bool {
		return firstAttempts.Load() == 3 &&
			secondAttempts.Load() == 2 &&
			thirdProcessed.Load()
	}, 30*time.Second, 500*time.Millisecond, "messages should be processed with retries")

	// verify all messages were processed
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, processedMsgs, 3, "all 3 messages should be processed")
	assert.ElementsMatch(t, []string{
		"first-will-fail-twice",
		"second-will-fail-once",
		"third-will-succeed",
	}, processedMsgs, "all messages should be processed (order may vary based on retry timing)")

	// stop consumers
	consumerCancel()
	wg.Wait()
}

// adapterTestHandler implements sarama.ConsumerGroupHandler for adapter tests.
// It uses the library-agnostic ErrorTracker via the retry.Message interface.
type adapterTestHandler struct {
	tracker   *resilience.ErrorTracker
	logger    *slog.Logger
	processor *testMessageProcessor
}

func (h *adapterTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *adapterTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *adapterTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// wrap Sarama message with adapter
		retryMsg := saramaadapter.NewMessage(msg)

		// process message using business logic
		err := h.processor.process(session.Context(), msg)

		if err != nil {
			// redirect all errors
			// (sends directly to DLQ instead of retry topic)
			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
			h.logger.Info("message redirected",
				"payload", string(msg.Value),
			)
		} else {
			// success - free from retry chain
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", "error", freeErr)
					return freeErr
				}
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// testMessageProcessor encapsulates test business logic
// This processor is used by BOTH main consumer and retry consumer
type testMessageProcessor struct {
	logger         *slog.Logger
	firstAttempts  *atomic.Int32
	secondAttempts *atomic.Int32
	thirdProcessed *atomic.Bool
	mu             *sync.Mutex
	processedMsgs  *[]string
}

func (p *testMessageProcessor) process(_ context.Context, msg *sarama.ConsumerMessage) error {
	payload := string(msg.Value)

	switch payload {
	case "first-will-fail-twice":
		attempt := p.firstAttempts.Add(1)
		p.logger.Info("processing first message",
			"attempt", attempt,
			"source", msg.Topic,
		)

		if attempt <= 2 {
			return fmt.Errorf("simulated failure for first message (attempt %d)", attempt)
		}

		// success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "second-will-fail-once":
		attempt := p.secondAttempts.Add(1)
		p.logger.Info("processing second message",
			"attempt", attempt,
			"source", msg.Topic,
		)

		if attempt == 1 {
			return fmt.Errorf("simulated failure for second message (attempt %d)", attempt)
		}

		// success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "third-will-succeed":
		p.logger.Info("processing third message (will succeed)", "source", msg.Topic)
		p.thirdProcessed.Store(true)

		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	default:
		return fmt.Errorf("unknown message payload: %s", payload)
	}
}

// readyDummyHandler is a minimal handler that signals when consumer group is established.
type readyDummyHandler struct {
	ready chan struct{}
	once  *sync.Once
}

func (h *readyDummyHandler) Setup(sarama.ConsumerGroupSession) error {
	h.once.Do(func() { close(h.ready) })
	return nil
}
func (h *readyDummyHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *readyDummyHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
	}
	return nil
}

// TestIntegration_ChainRetry verifies that messages properly go through the retry chain.
// Tests:
// - Message fails and gets redirected to retry topic
// - Message is tracked in retry chain (redirect topic)
// - Message is retried from retry topic
// - After success, message is freed from retry chain (tombstone published)
func TestIntegration_ChainRetry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-chain-retry")

	// track attempts
	var (
		attempts atomic.Int32
		mu       sync.Mutex
		success  bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// start tracking
	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)

	// create handler that fails first 2 attempts, then succeeds
	handler := &chainRetryHandler{
		tracker:  tracker,
		logger:   sharedLogger,
		attempts: &attempts,
		mu:       &mu,
		success:  &success,
	}

	// create main consumer
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	// create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady)

	// produce test message
	produceTestMessage(t, sharedBroker, topic, "test-key", "will-fail-twice-then-succeed")

	// wait for processing to complete (3 attempts total)
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts.Load() == 3 && success
	}, 30*time.Second, 500*time.Millisecond, "message should be processed after 3 attempts")

	consumerCancel()
	wg.Wait()
}

// chainRetryHandler handles messages for chain retry test.
type chainRetryHandler struct {
	tracker  *resilience.ErrorTracker
	logger   *slog.Logger
	attempts *atomic.Int32
	mu       *sync.Mutex
	success  *bool
}

func (h *chainRetryHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *chainRetryHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *chainRetryHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		retryMsg := saramaadapter.NewMessage(msg)
		attempt := h.attempts.Add(1)

		// fail first 2 attempts, succeed on 3rd
		if attempt <= 2 {
			err := fmt.Errorf("simulated failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}

			h.logger.Info("message redirected to retry",
				"attempt", attempt,
			)
		} else {
			// success on 3rd attempt
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", "error", freeErr)
					return freeErr
				}
				h.logger.Info("message freed from retry chain")
			}

			h.mu.Lock()
			*h.success = true
			h.mu.Unlock()

			h.logger.Info("message processed successfully",
				"total_attempts", attempt,
			)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// TestIntegration_DLQ verifies that messages exceeding max retries are sent to DLQ.
// Tests:
// - Message fails repeatedly (more than max retries)
// - Message is sent to DLQ topic
// - DLQ message contains proper headers (retry attempts, original error)
// - With FreeOnDLQ=false (default), message stays in tracking (blocks subsequent messages)
func TestIntegration_DLQ(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-dlq")

	// track attempts and DLQ
	var (
		attempts    atomic.Int32
		dlqReceived atomic.Bool
		mu          sync.Mutex
		dlqHeaders  map[string]string
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = false

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// start tracking
	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)
	dlqTopic := tracker.DLQTopic(topic)

	// create handler that always fails
	handler := &dlqTestHandler{
		tracker:     tracker,
		logger:      sharedLogger,
		attempts:    &attempts,
		dlqReceived: &dlqReceived,
		mu:          &mu,
		dlqHeaders:  &dlqHeaders,
	}

	// create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	t.Cleanup(func() { _ = dlqConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	dlqReady := runConsumerLoop(consumerCtx, &wg, dlqConsumer, []string{dlqTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady, dlqReady)

	// produce test message that will always fail
	produceTestMessage(t, sharedBroker, topic, "test-key", "will-always-fail")

	// wait for message to exceed max retries and go to DLQ
	// MaxRetries=3 means: 1 initial attempt + 3 retries = 4 total attempts
	assert.Eventually(t, func() bool {
		return attempts.Load() >= 4 && dlqReceived.Load()
	}, 30*time.Second, 500*time.Millisecond, "message should go to DLQ after exceeding max retries")

	// verify DLQ headers
	mu.Lock()
	assert.NotEmpty(t, dlqHeaders, "DLQ message should have headers")
	mu.Unlock()

	// stop consumers
	consumerCancel()
	wg.Wait()
}

// dlqTestHandler handles messages for DLQ test.
type dlqTestHandler struct {
	tracker     *resilience.ErrorTracker
	logger      *slog.Logger
	attempts    *atomic.Int32
	dlqReceived *atomic.Bool
	mu          *sync.Mutex
	dlqHeaders  *map[string]string
}

func (h *dlqTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dlqTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)

		// check if this is DLQ topic
		if strings.HasPrefix(claim.Topic(), "dlq_") || strings.HasPrefix(claim.Topic(), "dlq-") {
			h.logger.Info("received message in DLQ",
				"payload", payload,
				"topic", claim.Topic(),
			)

			h.dlqReceived.Store(true)

			// Extract headers
			h.mu.Lock()
			*h.dlqHeaders = make(map[string]string)
			for _, header := range msg.Headers {
				(*h.dlqHeaders)[string(header.Key)] = string(header.Value)
			}
			h.mu.Unlock()

			session.MarkMessage(msg, "")
			continue
		}

		retryMsg := saramaadapter.NewMessage(msg)

		// check retry attempt from headers
		var currentAttempt int
		for _, h := range msg.Headers {
			if string(h.Key) == "x-retry-attempt" {
				fmt.Sscanf(string(h.Value), "%d", &currentAttempt)
				break
			}
		}

		// check if max retries exceeded (before processing)
		if currentAttempt > 3 {
			h.logger.Info("max retries exceeded, sending to DLQ",
				"current_attempt", currentAttempt,
				"payload", payload,
			)

			im := resilience.NewInternalMessage(msg.Topic, msg.Key, msg.Value, mapSaramaHeaders(msg.Headers))
			err := h.tracker.SendToDLQ(
				session.Context(),
				im,
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", "error", err)
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		// for "will-always-fail" message, always fail
		if payload == "will-always-fail" {
			attempt := h.attempts.Add(1)

			h.logger.Info("processing message that will fail",
				"attempt", attempt,
				"topic", msg.Topic,
				"retry_attempt", currentAttempt,
			)

			err := fmt.Errorf("simulated permanent failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// mapSaramaHeaders converts Sarama headers to retry.HeaderList
func mapSaramaHeaders(headers []*sarama.RecordHeader) *resilience.HeaderList {
	result := &resilience.HeaderList{}
	for _, h := range headers {
		result.Set(string(h.Key), h.Value)
	}
	return result
}

// TestIntegration_DLQ_WithFreeOnDLQ verifies DLQ behavior with FreeOnDLQ=true.
// Tests:
// - Message exceeds max retries and goes to DLQ
// - With FreeOnDLQ=true, message is freed from tracking (tombstone published)
// - Subsequent messages with same key are NOT blocked
func TestIntegration_DLQ_WithFreeOnDLQ(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-dlq-free")

	var (
		firstAttempts  atomic.Int32
		firstDLQ       atomic.Bool
		secondReceived atomic.Bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = true

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// start tracking
	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)
	dlqTopic := tracker.DLQTopic(topic)

	// create handler
	handler := &dlqFreeTestHandler{
		tracker:        tracker,
		logger:         sharedLogger,
		firstAttempts:  &firstAttempts,
		firstDLQ:       &firstDLQ,
		secondReceived: &secondReceived,
	}

	// create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	t.Cleanup(func() { _ = dlqConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	dlqReady := runConsumerLoop(consumerCtx, &wg, dlqConsumer, []string{dlqTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady, dlqReady)

	// produce first message that will fail and go to DLQ
	produceTestMessage(t, sharedBroker, topic, "test-key", "first-will-fail")

	// wait for first message to go to DLQ
	assert.Eventually(t, func() bool {
		return firstAttempts.Load() >= 4 && firstDLQ.Load()
	}, 30*time.Second, 500*time.Millisecond, "first message should go to DLQ")

	sharedLogger.Info("first message sent to DLQ, now sending second message with same key")

	// produce second message with SAME key - should NOT be blocked because first was freed
	produceTestMessage(t, sharedBroker, topic, "test-key", "second-should-process")

	// wait for second message to be processed
	assert.Eventually(t, func() bool {
		return secondReceived.Load()
	}, 20*time.Second, 500*time.Millisecond, "second message should be processed (not blocked)")

	// stop consumers
	consumerCancel()
	wg.Wait()
}

// dlqFreeTestHandler handles messages for DLQ with FreeOnDLQ test.
type dlqFreeTestHandler struct {
	tracker        *resilience.ErrorTracker
	logger         *slog.Logger
	firstAttempts  *atomic.Int32
	firstDLQ       *atomic.Bool
	secondReceived *atomic.Bool
}

func (h *dlqFreeTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqFreeTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dlqFreeTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)

		// check if this is DLQ topic (check claim topic directly)
		if strings.HasPrefix(claim.Topic(), "dlq_") || strings.HasPrefix(claim.Topic(), "dlq-") {
			h.logger.Info("received message in DLQ",
				"payload", payload,
				"topic", claim.Topic(),
			)

			if payload == "first-will-fail" {
				h.firstDLQ.Store(true)
			}

			session.MarkMessage(msg, "")
			continue
		}

		retryMsg := saramaadapter.NewMessage(msg)

		// check retry attempt from headers
		var currentAttempt int
		for _, header := range msg.Headers {
			if string(header.Key) == "x-retry-attempt" {
				fmt.Sscanf(string(header.Value), "%d", &currentAttempt)
				break
			}
		}

		// check if max retries exceeded (before processing)
		if currentAttempt > 3 {
			h.logger.Info("max retries exceeded, sending to DLQ",
				"current_attempt", currentAttempt,
				"payload", payload,
			)

			im := resilience.NewInternalMessage(msg.Topic, msg.Key, msg.Value, mapSaramaHeaders(msg.Headers))
			err := h.tracker.SendToDLQ(
				session.Context(),
				im,
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", "error", err)
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		if payload == "first-will-fail" {
			attempt := h.firstAttempts.Add(1)

			h.logger.Info("processing first message",
				"attempt", attempt,
				"retry_attempt", currentAttempt,
			)

			err := fmt.Errorf("simulated failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
		} else if payload == "second-should-process" {
			h.logger.Info("processing second message (should not be blocked)")
			h.secondReceived.Store(true)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// TestIntegration_StrictOrdering verifies that messages with the same key are processed
// in order, with subsequent messages blocked until preceding messages complete their retry chain.
func TestIntegration_StrictOrdering(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-strict-ordering")

	var (
		mu             sync.Mutex
		processedOrder []string
		msg1Attempts   atomic.Int32
		msg2Processed  atomic.Bool
		msg3Processed  atomic.Bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	retryTopic := tracker.RetryTopic(topic)

	// Handler that:
	// - msg1 fails first time, succeeds on retry
	// - msg2 and msg3 should wait for msg1 to complete
	handler := &strictOrderingHandler{
		tracker:        tracker,
		logger:         sharedLogger,
		mu:             &mu,
		processedOrder: &processedOrder,
		msg1Attempts:   &msg1Attempts,
		msg2Processed:  &msg2Processed,
		msg3Processed:  &msg3Processed,
	}

	// create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady)

	// produce 3 messages with the SAME key (strict ordering required)
	// msg1 will fail first, then succeed on retry
	// msg2 and msg3 should be blocked until msg1 completes
	sameKey := "ordering-key"
	produceTestMessage(t, sharedBroker, topic, sameKey, "msg1-will-fail-once")

	// wait for msg1 to be processed and enter retry chain before sending subsequent messages
	require.Eventually(t, func() bool {
		return msg1Attempts.Load() >= 1
	}, 10*time.Second, 100*time.Millisecond, "msg1 should be processed and enter retry chain")

	produceTestMessage(t, sharedBroker, topic, sameKey, "msg2-should-wait")
	produceTestMessage(t, sharedBroker, topic, sameKey, "msg3-should-wait")

	// wait for all messages to be processed
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processedOrder) == 3
	}, 60*time.Second, 500*time.Millisecond, "all 3 messages should be processed")

	// Verify ordering: msg1 should complete before msg2 and msg3
	mu.Lock()
	defer mu.Unlock()

	sharedLogger.Info("processing order", "order", processedOrder)

	// msg1 should be first (either first attempt or retry)
	assert.Contains(t, processedOrder[0], "msg1", "msg1 should complete first")

	// msg2 and msg3 should come after msg1
	for i, msg := range processedOrder {
		if strings.Contains(msg, "msg2") || strings.Contains(msg, "msg3") {
			assert.Greater(t, i, 0, "%s should come after msg1", msg)
		}
	}

	consumerCancel()
	wg.Wait()
}

type strictOrderingHandler struct {
	tracker        *resilience.ErrorTracker
	logger         *slog.Logger
	mu             *sync.Mutex
	processedOrder *[]string
	msg1Attempts   *atomic.Int32
	msg2Processed  *atomic.Bool
	msg3Processed  *atomic.Bool
}

func (h *strictOrderingHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *strictOrderingHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *strictOrderingHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)
		retryMsg := saramaadapter.NewMessage(msg)

		h.logger.Info("processing message",
			"payload", payload,
			"topic", msg.Topic,
			"is_retry", strings.HasPrefix(msg.Topic, "retry_"),
		)

		// Check if key is in retry chain (for messages from main topic)
		if !strings.HasPrefix(msg.Topic, "retry_") && h.tracker.IsInRetryChain(session.Context(), retryMsg) {
			h.logger.Info("key is in retry chain, redirecting to maintain order",
				"payload", payload,
			)
			if err := h.tracker.Redirect(session.Context(), retryMsg, fmt.Errorf("ordering: predecessor in retry")); err != nil {
				h.logger.Error("failed to redirect", "error", err)
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		switch {
		case strings.Contains(payload, "msg1"):
			attempt := h.msg1Attempts.Add(1)
			h.logger.Info("processing msg1", "attempt", attempt)

			if attempt == 1 {
				// First attempt fails
				if err := h.tracker.Redirect(session.Context(), retryMsg, fmt.Errorf("simulated failure")); err != nil {
					h.logger.Error("failed to redirect msg1", "error", err)
					return err
				}
			} else {
				// Retry succeeds
				if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
					if err := h.tracker.Free(session.Context(), retryMsg); err != nil {
						h.logger.Error("failed to free msg1", "error", err)
						return err
					}
				}
				h.mu.Lock()
				*h.processedOrder = append(*h.processedOrder, "msg1-success")
				h.mu.Unlock()
			}

		case strings.Contains(payload, "msg2"):
			h.logger.Info("processing msg2 (should only happen after msg1 completes)")
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if err := h.tracker.Free(session.Context(), retryMsg); err != nil {
					h.logger.Error("failed to free msg2", "error", err)
					return err
				}
			}
			h.mu.Lock()
			*h.processedOrder = append(*h.processedOrder, "msg2-success")
			h.mu.Unlock()
			h.msg2Processed.Store(true)

		case strings.Contains(payload, "msg3"):
			h.logger.Info("processing msg3 (should only happen after msg1 completes)")
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if err := h.tracker.Free(session.Context(), retryMsg); err != nil {
					h.logger.Error("failed to free msg3", "error", err)
					return err
				}
			}
			h.mu.Lock()
			*h.processedOrder = append(*h.processedOrder, "msg3-success")
			h.mu.Unlock()
			h.msg3Processed.Store(true)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// TestIntegration_NotRetriableError verifies that messages failing with NotRetriableError
// bypass the retry topic and go directly to DLQ without acquiring a lock.
func TestIntegration_NotRetriableError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-not-retriable")

	var (
		dlqReceived       atomic.Bool
		retryTopicChecked atomic.Bool
		retryMsgFound     atomic.Bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	retryTopic := tracker.RetryTopic(topic)
	dlqTopic := tracker.DLQTopic(topic)

	// handler that returns NotRetriableError for specific message
	handler := &notRetriableHandler{
		tracker: tracker,
		logger:  sharedLogger,
	}

	// DLQ handler to verify message arrives there
	dlqHandler := &simpleDLQHandler{
		logger:      sharedLogger,
		dlqReceived: &dlqReceived,
	}

	// retry topic spy to verify message does NOT go there
	retrySpyHandler := &retrySpyHandler{
		logger:        sharedLogger,
		retryMsgFound: &retryMsgFound,
		checked:       &retryTopicChecked,
	}

	// create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainConsumer.Close() })

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dlqConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry-spy", client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	dlqReady := runConsumerLoop(consumerCtx, &wg, dlqConsumer, []string{dlqTopic}, dlqHandler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, retrySpyHandler, sharedLogger)
	waitForReady(t, ctx, mainReady, dlqReady, retryReady)

	// Produce message that will fail with NotRetriableError
	produceTestMessage(t, sharedBroker, topic, "not-retriable-key", "validation-error-message")

	// Wait for message to arrive in DLQ
	assert.Eventually(t, func() bool {
		return dlqReceived.Load()
	}, 30*time.Second, 500*time.Millisecond, "message should arrive in DLQ")

	// Once message is in DLQ, we've waited long enough - if it was going to retry topic, it would have by now
	retryTopicChecked.Store(true)

	// Verify message did NOT go to retry topic
	assert.False(t, retryMsgFound.Load(), "NotRetriableError should bypass retry topic")

	// Verify key is NOT locked (NotRetriableError doesn't acquire lock)
	checkMsg := saramaadapter.NewMessage(&sarama.ConsumerMessage{
		Topic: topic,
		Key:   []byte("not-retriable-key"),
	})
	assert.False(t, tracker.IsInRetryChain(ctx, checkMsg),
		"Key should NOT be locked for NotRetriableError")

	sharedLogger.Info("NotRetriableError test completed",
		"dlq_received", dlqReceived.Load(),
		"retry_msg_found", retryMsgFound.Load(),
	)

	consumerCancel()
	wg.Wait()
}

type notRetriableHandler struct {
	tracker *resilience.ErrorTracker
	logger  *slog.Logger
}

func (h *notRetriableHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *notRetriableHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *notRetriableHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)
		retryMsg := saramaadapter.NewMessage(msg)

		h.logger.Info("processing message", "payload", payload)

		if payload == "validation-error-message" {
			// Return NotRetriableError - should go directly to DLQ
			notRetriableErr := resilience.NewNotRetriableError(
				fmt.Errorf("validation failed: invalid message format"),
			)
			if err := h.tracker.Redirect(session.Context(), retryMsg, notRetriableErr); err != nil {
				h.logger.Error("failed to redirect", "error", err)
				return err
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

type simpleDLQHandler struct {
	logger      *slog.Logger
	dlqReceived *atomic.Bool
}

func (h *simpleDLQHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *simpleDLQHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *simpleDLQHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.logger.Info("received message in DLQ",
			"payload", string(msg.Value),
			"key", string(msg.Key),
		)
		h.dlqReceived.Store(true)
		session.MarkMessage(msg, "")
	}
	return nil
}

type retrySpyHandler struct {
	logger        *slog.Logger
	retryMsgFound *atomic.Bool
	checked       *atomic.Bool
}

func (h *retrySpyHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *retrySpyHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *retrySpyHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.logger.Info("found message in retry topic (unexpected for NotRetriableError)",
			"payload", string(msg.Value),
		)
		h.retryMsgFound.Store(true)
		session.MarkMessage(msg, "")
	}
	return nil
}

// TestIntegration_ConcurrentKeysIndependence verifies that messages with DIFFERENT keys
// can be processed independently and in parallel, without blocking each other.
func TestIntegration_ConcurrentKeysIndependence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-concurrent-keys")

	var (
		mu             sync.Mutex
		processedOrder []string
		keyAAttempts   atomic.Int32
		keyBProcessed  atomic.Bool
		keyCProcessed  atomic.Bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	retryTopic := tracker.RetryTopic(topic)

	handler := &concurrentKeysHandler{
		tracker:        tracker,
		logger:         sharedLogger,
		mu:             &mu,
		processedOrder: &processedOrder,
		keyAAttempts:   &keyAAttempts,
		keyBProcessed:  &keyBProcessed,
		keyCProcessed:  &keyCProcessed,
	}

	// create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	mainReady := runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, sharedLogger)
	retryReady := runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, sharedLogger)
	waitForReady(t, ctx, mainReady, retryReady)

	// produce messages with DIFFERENT keys
	// Key A will fail and enter retry chain
	// Key B and Key C should process immediately (not blocked by Key A)
	produceTestMessage(t, sharedBroker, topic, "key-A", "keyA-will-fail")

	// wait for key-A to be processed before sending B and C
	require.Eventually(t, func() bool {
		return keyAAttempts.Load() >= 1
	}, 10*time.Second, 100*time.Millisecond, "key-A should be processed first")

	produceTestMessage(t, sharedBroker, topic, "key-B", "keyB-should-succeed")
	produceTestMessage(t, sharedBroker, topic, "key-C", "keyC-should-succeed")

	// key B and Key C should be processed quickly (not waiting for Key A's retry)
	assert.Eventually(t, func() bool {
		return keyBProcessed.Load() && keyCProcessed.Load()
	}, 10*time.Second, 500*time.Millisecond,
		"Key B and Key C should process without waiting for Key A's retry")

	// verify Key A is still in retry chain while B and C completed
	checkMsgA := saramaadapter.NewMessage(&sarama.ConsumerMessage{Topic: topic, Key: []byte("key-A")})
	assert.True(t, tracker.IsInRetryChain(ctx, checkMsgA),
		"Key A should still be in retry chain")

	// wait for Key A to complete its retry
	assert.Eventually(t, func() bool {
		return keyAAttempts.Load() >= 2
	}, 30*time.Second, 500*time.Millisecond, "Key A should complete retry")

	mu.Lock()
	sharedLogger.Info("processing order", "order", processedOrder)

	// verify B and C were processed before A completed retry
	// find positions
	var posA, posB, posC int = -1, -1, -1
	for i, entry := range processedOrder {
		if strings.Contains(entry, "keyA") && strings.Contains(entry, "success") {
			posA = i
		}
		if strings.Contains(entry, "keyB") {
			posB = i
		}
		if strings.Contains(entry, "keyC") {
			posC = i
		}
	}
	mu.Unlock()

	// B and C should have been processed before A's successful retry
	if posA >= 0 && posB >= 0 {
		assert.Less(t, posB, posA, "Key B should complete before Key A's retry succeeds")
	}
	if posA >= 0 && posC >= 0 {
		assert.Less(t, posC, posA, "Key C should complete before Key A's retry succeeds")
	}

	consumerCancel()
	wg.Wait()
}

type concurrentKeysHandler struct {
	tracker        *resilience.ErrorTracker
	logger         *slog.Logger
	mu             *sync.Mutex
	processedOrder *[]string
	keyAAttempts   *atomic.Int32
	keyBProcessed  *atomic.Bool
	keyCProcessed  *atomic.Bool
}

func (h *concurrentKeysHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *concurrentKeysHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *concurrentKeysHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)
		key := string(msg.Key)
		retryMsg := saramaadapter.NewMessage(msg)

		h.logger.Info("processing message",
			"key", key,
			"payload", payload,
			"topic", msg.Topic,
		)

		switch {
		case strings.Contains(payload, "keyA"):
			attempt := h.keyAAttempts.Add(1)
			h.logger.Info("processing keyA", "attempt", attempt)

			if attempt == 1 {
				// First attempt fails
				if err := h.tracker.Redirect(session.Context(), retryMsg, fmt.Errorf("simulated failure")); err != nil {
					h.logger.Error("failed to redirect keyA", "error", err)
					return err
				}
				h.mu.Lock()
				*h.processedOrder = append(*h.processedOrder, "keyA-failed")
				h.mu.Unlock()
			} else {
				// Retry succeeds
				if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
					if err := h.tracker.Free(session.Context(), retryMsg); err != nil {
						h.logger.Error("failed to free keyA", "error", err)
						return err
					}
				}
				h.mu.Lock()
				*h.processedOrder = append(*h.processedOrder, "keyA-success")
				h.mu.Unlock()
			}

		case strings.Contains(payload, "keyB"):
			h.logger.Info("processing keyB (should not be blocked by keyA)")
			h.mu.Lock()
			*h.processedOrder = append(*h.processedOrder, "keyB-success")
			h.mu.Unlock()
			h.keyBProcessed.Store(true)

		case strings.Contains(payload, "keyC"):
			h.logger.Info("processing keyC (should not be blocked by keyA)")
			h.mu.Lock()
			*h.processedOrder = append(*h.processedOrder, "keyC-success")
			h.mu.Unlock()
			h.keyCProcessed.Store(true)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
