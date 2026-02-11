// Package main provides an interactive HTTP demo for kafka-resilience.
// Users can send messages, control failures, and observe ordering guarantees.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

var (
	producer sarama.SyncProducer
	tracker  *resilience.ErrorTracker
	handler  *DemoHandler
	topic    = "demo-orders"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	brokers := []string{"127.0.0.1:9092"}
	if env := os.Getenv("KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}
	groupID := "demo-group"

	slog.Info("Starting Interactive Demo", "brokers", brokers, "topic", topic)

	// Setup Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		slog.Error("Failed to create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	producer, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		slog.Error("Failed to create producer", "error", err)
		os.Exit(1)
	}

	// Setup resilience tracker
	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	tracker, err = saramaadapter.NewResilienceTracker(cfg, client, resilience.WithLogger(slog.Default()))
	if err != nil {
		slog.Error("Failed to create tracker", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Create handler with fail control
	handler = NewDemoHandler(tracker)

	// Start tracker and consumer
	if err := tracker.Start(ctx, topic, handler); err != nil {
		slog.Error("Failed to start tracker", "error", err)
		os.Exit(1)
	}

	consumerFactory := saramaadapter.NewConsumerFactory(client)
	consumer, err := consumerFactory.NewConsumer(groupID)
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx, []string{topic}, tracker.NewResilientHandler(handler)); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("Consumer error", "error", err)
			}
		}
	}()

	// Setup HTTP server
	http.HandleFunc("/produce", handleProduce)
	http.HandleFunc("/locks", handleLocks)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/fail", handleFail)

	server := &http.Server{Addr: ":8080"}

	go func() {
		slog.Info("HTTP server started", "addr", "http://localhost:8080")
		slog.Info("Endpoints: POST /produce, GET /locks, GET /stats, POST /fail")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutting down...")
	server.Shutdown(context.Background())
	wg.Wait()
}

// ProduceRequest is the request body for POST /produce
type ProduceRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Fail  bool   `json:"fail"` // If true, message will fail on first attempt
}

// handleProduce sends a message to Kafka
func handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// If fail=true, register this key to fail on next processing
	if req.Fail {
		handler.SetFail(req.Key)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(req.Key),
		Value: sarama.StringEncoder(req.Value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to produce: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"status":    "produced",
		"key":       req.Key,
		"partition": partition,
		"offset":    offset,
		"will_fail": req.Fail,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	slog.Info("Produced message", "key", req.Key, "will_fail", req.Fail)
}

// handleLocks returns currently locked keys
func handleLocks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	lockedKeys := handler.GetLockedKeys()
	pendingFails := handler.GetPendingFails()

	resp := map[string]any{
		"locked_keys":   lockedKeys,
		"pending_fails": pendingFails,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleStats returns processing statistics
func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := handler.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// FailRequest sets keys that should fail
type FailRequest struct {
	Key  string `json:"key"`
	Fail bool   `json:"fail"` // true to make it fail, false to clear
}

// handleFail controls which keys will fail
func handleFail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Fail {
		handler.SetFail(req.Key)
	} else {
		handler.ClearFail(req.Key)
	}

	resp := map[string]any{
		"key":       req.Key,
		"will_fail": req.Fail,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
