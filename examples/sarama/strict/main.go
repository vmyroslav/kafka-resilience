// Package main demonstrates strict ordering guarantees with kafka-resilience.
// Uses Synchronize() during consumer group rebalancing to ensure local state is fully synced before processing,
// preventing race conditions during partition reassignment.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/IBM/sarama"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

func main() {
	// 1. Configuration
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "example-strict-topic")
	groupID := getEnv("KAFKA_GROUP_ID", "example-strict-group")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 2. Setup Sarama Client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		logger.Error("Failed to create Sarama client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// 3. Configure Resilience Tracker
	resilienceCfg := resilience.NewDefaultConfig()
	resilienceCfg.GroupID = groupID
	resilienceCfg.MaxRetries = 3
	resilienceCfg.RetryTopicPartitions = 1

	tracker, err := saramaadapter.NewResilienceTracker(resilienceCfg, client, resilience.WithLogger(logger))
	if err != nil {
		logger.Error("Failed to create resilience tracker", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// 4. Start Tracker
	if err := tracker.Start(ctx, topic, &myBusinessHandler{logger: logger}); err != nil {
		logger.Error("Failed to start tracker", "error", err)
		os.Exit(1)
	}

	// 5. Start Main Consumer
	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		logger.Error("Failed to create consumer group", "error", err)
		os.Exit(1)
	}
	defer consumerGroup.Close()

	// Wrap the business handler with the resilience logic
	strictHandler := &StrictConsumerGroupHandler{
		tracker: tracker,
		handler: tracker.NewResilientHandler(&myBusinessHandler{logger: logger}), // The resilient handler wraps our business logic
		logger:  logger,
	}

	logger.Info("Starting Strict Consumer...", "topic", topic)

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{topic}, strictHandler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.Error("Consumer loop error", "error", err)
				time.Sleep(time.Second)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-ctx.Done()
	logger.Info("Shutting down...")
}

// StrictConsumerGroupHandler implements sarama.ConsumerGroupHandler and performs synchronization on rebalance.
type StrictConsumerGroupHandler struct {
	tracker *resilience.ErrorTracker
	handler resilience.ConsumerHandler
	logger  *slog.Logger
}

func (h *StrictConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("Rebalance detected: Synchronizing state...", "member_id", session.MemberID())

	// THIS IS THE KEY LINE:
	// We block processing until our internal state matches the distributed log.
	if err := h.tracker.Synchronize(session.Context()); err != nil {
		return err
	}

	h.logger.Info("State synchronized. Ready to consume.")
	return nil
}

func (h *StrictConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *StrictConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Convert Sarama message
		retryMsg := saramaadapter.NewMessage(msg)

		// Process via resilient handler (which handles ordering & retries)
		if err := h.handler.Handle(session.Context(), retryMsg); err != nil {
			// The resilience handler usually absorbs errors by redirecting to retry.
			// If it returns an error here, it means something critical failed (e.g. producing to retry topic).
			h.logger.Error("Handler failed processing message", "error", err)
			return err
		}

		// Mark as processed
		session.MarkMessage(msg, "")
	}

	return nil
}

// myBusinessHandler is the core logic
type myBusinessHandler struct {
	logger *slog.Logger
}

func (h *myBusinessHandler) Handle(ctx context.Context, msg resilience.Message) error {
	val := string(msg.Value())
	h.logger.Info("Processing message", "key", string(msg.Key()), "value", val)

	// Simulate random failure
	if strings.Contains(val, "fail") {
		return errors.New("simulated failure")
	}

	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
