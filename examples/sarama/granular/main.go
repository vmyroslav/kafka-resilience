// Package main demonstrates granular control over retry handling.
// The user manages both main and retry consumers explicitly,
// calling Redirect() and Free() for full control.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	brokers := []string{"127.0.0.1:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	topic := "example-orders-manual"
	groupID := "example-orders-manual-group"

	slog.Info("Starting Kafka Resilience Manual Example")
	slog.Info("Configuration", "brokers", brokers)

	// 1. Setup Sarama Client
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		slog.Error("Error creating Sarama client", "error", err)
		return
	}
	defer client.Close()

	// 2. Create ErrorTracker
	resilienceCfg := resilience.NewDefaultConfig()
	resilienceCfg.GroupID = groupID
	resilienceCfg.MaxRetries = 3
	resilienceCfg.RetryTopicPartitions = 1

	tracker, err := saramaadapter.NewResilienceTracker(
		resilienceCfg,
		client,
		resilience.WithLogger(slog.Default()),
	)
	if err != nil {
		slog.Error("Failed to create tracker", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Start tracking only (no retry worker)
	// This initializes the coordinator and restores state from the redirect topic
	if err := tracker.StartCoordinator(ctx, topic); err != nil {
		slog.Error("Failed to start tracking", "error", err)
		os.Exit(1)
	}

	// 4. Create manual handler that explicitly calls Redirect/Free
	handler := &ManualHandler{tracker: tracker, originalTopic: topic}

	// 5. Create adapters for manual consumer management
	consumerFactory := saramaadapter.NewConsumerFactory(client)

	// 6. Create MAIN consumer
	mainConsumer, err := consumerFactory.NewConsumer(groupID)
	if err != nil {
		slog.Error("Failed to create main consumer", "error", err)
		os.Exit(1)
	}
	defer mainConsumer.Close()

	// 7. Create RETRY consumer (user manages this explicitly in manual mode)
	retryConsumer, err := consumerFactory.NewConsumer(groupID + "-retry")
	if err != nil {
		slog.Error("Failed to create retry consumer", "error", err)
		os.Exit(1)
	}
	defer retryConsumer.Close()

	retryTopic := tracker.RetryTopic(topic)
	slog.Info("Starting consumers", "main_topic", topic, "retry_topic", retryTopic)

	var wg sync.WaitGroup

	// 8. Start MAIN consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Main consumer started", "topic", topic)
		if err := mainConsumer.Consume(ctx, []string{topic}, handler); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("Main consumer error", "error", err)
			}
		}
	}()

	// 9. Start RETRY consumer goroutine (same handler, different topic)
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Retry consumer started", "topic", retryTopic)
		if err := retryConsumer.Consume(ctx, []string{retryTopic}, handler); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("Retry consumer error", "error", err)
			}
		}
	}()

	// 10. Produce sample messages
	go produceSampleMessages(client, topic)

	// 11. Wait for shutdown signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	slog.Info("Shutting down...")

	cancel()
	wg.Wait()
}

// ManualHandler demonstrates explicit Redirect/Free calls.
type ManualHandler struct {
	tracker       *resilience.ErrorTracker
	originalTopic string
}

func (h *ManualHandler) Handle(ctx context.Context, msg resilience.Message) error {
	key := string(msg.Key())
	val := string(msg.Value())
	attempt := 0
	if attemptBytes, ok := msg.Headers().Get(resilience.HeaderRetryAttempt); ok {
		attempt, _ = strconv.Atoi(string(attemptBytes))
	}

	isRetry := attempt > 0

	slog.Info("Processing message",
		"key", key,
		"value", val,
		"is_retry", isRetry,
		"attempt", attempt,
		"topic", msg.Topic(),
	)

	// Manual ordering check for main topic messages
	if !isRetry {
		// Check if this key is already in retry chain (from a previous message)
		if h.tracker.IsInRetryChain(ctx, msg) {
			slog.Warn("Key is in retry chain, redirecting to maintain order", "key", key)
			return h.tracker.Redirect(ctx, msg, errors.New("strict ordering: predecessor in retry"))
		}
	}

	// Simulate processing
	if val == "fail" {
		slog.Warn("Processing failed", "key", key, "attempt", attempt)
		// Explicit Redirect on failure
		return h.tracker.Redirect(ctx, msg, errors.New("simulated error"))
	}

	// Success
	slog.Info("Successfully processed", "key", key)

	// Explicit Free on success (only for retry messages)
	if isRetry {
		if err := h.tracker.Free(ctx, msg); err != nil {
			slog.Error("Failed to free message", "key", key, "error", err)
			return err
		}
		slog.Info("Released lock", "key", key)
	}

	return nil
}

func produceSampleMessages(client sarama.Client, topic string) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		slog.Error("Failed to create sample producer", "error", err)
		return
	}

	messages := []struct {
		key, val string
	}{
		{"order-1", "success"},
		{"order-2", "fail"}, // will retry 3 times then go to DLQ
		{"order-3", "success"},
		{"order-2", "success"}, // blocked until order-2 retry completes or DLQ
	}

	for _, m := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(m.key),
			Value: sarama.StringEncoder(m.val),
		}
		slog.Info("Producing message", "key", m.key, "value", m.val)
		if _, _, err := producer.SendMessage(msg); err != nil {
			slog.Error("Failed to produce", "key", m.key, "error", err)
		}
	}
}
