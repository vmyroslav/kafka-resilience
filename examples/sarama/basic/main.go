// Package main demonstrates automatic retry handling with kafka-resilience.
// This is the default pattern for most use cases.
// The library manages the retry worker and handles Redirect/Free calls internally.
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
	// set default logger
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// assume Kafka is running on localhost:9092
	brokers := []string{"127.0.0.1:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}
	topic := "example-orders"
	groupID := "example-orders-group"

	slog.Info("Starting Kafka Resilience Sarama Example")
	slog.Info("Configuration", "brokers", brokers)

	// 1. Setup Sarama Client
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		slog.Error("Error creating Sarama client (is Kafka running?)", "error", err)
		return
	}
	defer client.Close()

	// initialize config
	resilienceCfg := resilience.NewDefaultConfig()
	resilienceCfg.GroupID = groupID
	resilienceCfg.MaxRetries = 3
	resilienceCfg.RetryTopicPartitions = 1 // simplified for example

	// NewResilienceTracker automatically wires up all default adapters and the coordinator
	tracker, err := saramaadapter.NewResilienceTracker(resilienceCfg, client, resilience.WithLogger(slog.Default()))
	if err != nil {
		slog.Error("failed to create resilience manager", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start tracker
	handler := &OrderHandler{}
	if err := tracker.Start(ctx, topic, handler); err != nil {
		slog.Error("failed to start error tracker", "error", err)
		os.Exit(1)
	}

	// start main consumer
	// We use the factory from the adapter package, but you could also create your own Sarama consumer.
	// The important part is using tracker.NewResilientHandler(handler).
	consumerFactory := saramaadapter.NewConsumerFactory(client)
	appConsumer, err := consumerFactory.NewConsumer(groupID)
	if err != nil {
		slog.Error("failed to create main consumer", "error", err)
		os.Exit(1)
	}
	defer appConsumer.Close()

	slog.Info("Starting Main Consumer", "topic", topic)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// resilientHandler automatically handles strict ordering and redirection
		if err := appConsumer.Consume(ctx, []string{topic}, tracker.NewResilientHandler(handler)); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("Consumer loop error", "error", err)
			}
		}
	}()

	// produce sample messages
	go produceSampleMessages(client, topic)

	// wait for shutdown signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	slog.Info("Shutting down...")

	cancel()
	wg.Wait()
}

// OrderHandler processes messages and uses the tracker for resilience.
type OrderHandler struct{}

func (h *OrderHandler) Handle(_ context.Context, msg resilience.Message) error {
	key := string(msg.Key())
	val := string(msg.Value())

	// check attempt count just for visibility
	attempt := 0
	if attemptBytes, ok := msg.Headers().Get(resilience.HeaderRetryAttempt); ok {
		attempt, _ = strconv.Atoi(string(attemptBytes))
	}

	slog.Info("Processing message", "key", key, "value", val, "attempt", attempt)

	if val == "fail" {
		// SIMULATED FAILURE
		slog.Warn("Processing failed", "key", key)
		return errors.New("simulated error")
	}

	// SUCCESS
	slog.Info("Successfully processed", "key", key)

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
		{"order-2", "fail"}, // should retry 3 times
		{"order-3", "success"},
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
