package resilience

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config holds configuration for the retry mechanism.
type Config struct {
	// RetryTopicPrefix is the prefix for retry topics where failed messages are sent for reprocessing.
	// Topic name format: {prefix}_{original_topic} (e.g., "retry_orders").
	RetryTopicPrefix string

	// RedirectTopicPrefix is the prefix for redirect topics used for distributed state tracking.
	// These compacted topics store locks to ensure message ordering guarantees during retries.
	// Topic name format: {prefix}_{original_topic} (e.g., "redirect_orders").
	RedirectTopicPrefix string

	// DLQTopicPrefix is the prefix for dead-letter queue topics where messages that exceed
	// max retries are sent for manual inspection and recovery.
	// Topic name format: {prefix}_{original_topic} (e.g., "dlq_orders").
	DLQTopicPrefix string

	// GroupID is the consumer group ID for the retry worker.
	// This is a required field and must be unique per application instance.
	GroupID string

	// MaxRetries is the maximum number of retry attempts before sending a message to the DLQ.
	// Default: 5
	MaxRetries int

	// InitialOffset specifies where to start consuming from retry and redirect topics.
	// -1 = newest, -2 = oldest.
	// Default: -2 (OffsetOldest)
	InitialOffset int64

	// StateRestoreTimeout is the maximum time to wait for the redirect topic
	// state restoration to complete before starting message processing.
	// Default: 30s
	StateRestoreTimeout time.Duration

	// StateRestoreIdleTimeout is the idle time to wait after the last message
	// during state restoration before considering the restoration complete.
	// Default: 5s
	StateRestoreIdleTimeout time.Duration

	// RetryTopicPartitions specifies the number of partitions for auto-created retry topics.
	// 0 means use the same number of partitions as the original topic.
	// Default: 0
	RetryTopicPartitions int32

	// ReplicationFactor specifies the replication factor for auto-created topics (retry, redirect, DLQ).
	// For production environments, this should typically be set higher than 1 for fault tolerance.
	// Default: 1
	ReplicationFactor int16

	// FreeOnDLQ determines whether to release locks when a message is sent to DLQ.
	// If false, locks remain until manually cleared, preserving ordering guarantees.
	// Default: false
	FreeOnDLQ bool

	// WorkerRestartInterval is the delay before restarting a background consumer (retry worker
	// or redirect consumer) after it exits with an error.
	// Default: 5s
	WorkerRestartInterval time.Duration

	// DisableAutoTopicCreation disables automatic creation of retry, redirect, and DLQ topics.
	// If true, topics must be created manually before use.
	// Default: false
	DisableAutoTopicCreation bool
}

// NewDefaultConfig creates a Config with sensible defaults.
func NewDefaultConfig() *Config {
	return &Config{
		RedirectTopicPrefix:      "redirect",
		DLQTopicPrefix:           "dlq",
		RetryTopicPrefix:         "retry",
		MaxRetries:               5,
		RetryTopicPartitions:     0,
		ReplicationFactor:        1,
		InitialOffset:            -2, // OffsetOldest
		FreeOnDLQ:                false,
		StateRestoreTimeout:      30 * time.Second,
		StateRestoreIdleTimeout:  5 * time.Second,
		WorkerRestartInterval:    5 * time.Second,
		DisableAutoTopicCreation: false,
	}
}

// Validate checks the configuration for errors and returns an error if any field is invalid.
func (c *Config) Validate() error {
	var errs []string

	errs = c.validateRequiredFields(errs)
	errs = c.validateNumericFields(errs)
	errs = c.validatePrefixCollisions(errs)

	if len(errs) > 0 {
		return errors.New("config validation failed: " + strings.Join(errs, "; "))
	}

	return nil
}

func (c *Config) validateRequiredFields(errs []string) []string {
	if c.GroupID == "" {
		errs = append(errs, "GroupID is required")
	}

	if c.RetryTopicPrefix == "" {
		errs = append(errs, "RetryTopicPrefix cannot be empty")
	}

	if c.RedirectTopicPrefix == "" {
		errs = append(errs, "RedirectTopicPrefix cannot be empty")
	}

	if c.DLQTopicPrefix == "" {
		errs = append(errs, "DLQTopicPrefix cannot be empty")
	}

	return errs
}

func (c *Config) validateNumericFields(errs []string) []string {
	if c.MaxRetries < 0 {
		errs = append(errs, fmt.Sprintf("MaxRetries must be >= 0, got %d", c.MaxRetries))
	}

	if c.ReplicationFactor < 1 {
		errs = append(errs, fmt.Sprintf("ReplicationFactor must be >= 1, got %d", c.ReplicationFactor))
	}

	if c.RetryTopicPartitions < 0 {
		errs = append(errs, fmt.Sprintf("RetryTopicPartitions must be >= 0, got %d", c.RetryTopicPartitions))
	}

	if c.StateRestoreTimeout < 0 {
		errs = append(errs, fmt.Sprintf("StateRestoreTimeout must be >= 0, got %v", c.StateRestoreTimeout))
	}

	if c.StateRestoreIdleTimeout < 0 {
		errs = append(errs, fmt.Sprintf("StateRestoreIdleTimeout must be >= 0, got %v", c.StateRestoreIdleTimeout))
	}

	if c.WorkerRestartInterval < 0 {
		errs = append(errs, fmt.Sprintf("WorkerRestartInterval must be >= 0, got %v", c.WorkerRestartInterval))
	}

	return errs
}

func (c *Config) validatePrefixCollisions(errs []string) []string {
	if c.RetryTopicPrefix == c.RedirectTopicPrefix {
		errs = append(errs, "RetryTopicPrefix and RedirectTopicPrefix must be different")
	}

	if c.RetryTopicPrefix == c.DLQTopicPrefix {
		errs = append(errs, "RetryTopicPrefix and DLQTopicPrefix must be different")
	}

	if c.RedirectTopicPrefix == c.DLQTopicPrefix {
		errs = append(errs, "RedirectTopicPrefix and DLQTopicPrefix must be different")
	}

	return errs
}
