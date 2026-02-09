package resilience

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KafkaStateCoordinator implements StateCoordinator using a compacted Kafka topic and local memory.
// It decorates localStateCoordinator to add distributed synchronization.
type KafkaStateCoordinator struct {
	cfg *Config

	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	logger          Logger
	local           *localStateCoordinator
	instruments     *coordinatorInstruments

	errCh           chan error
	consumedOffsets map[int32]int64
	instanceID      string
	topic           string

	mu          sync.RWMutex
	offsetsCond *sync.Cond // signals when consumedOffsets are updated
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewKafkaStateCoordinator creates a coordinator using a compacted Kafka topic for distributed state.
func NewKafkaStateCoordinator(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	meter metric.Meter,
) *KafkaStateCoordinator {
	k := &KafkaStateCoordinator{
		local:           newLocalStateCoordinator(),
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		cfg:             cfg,
		logger:          logger,
		errCh:           make(chan error, 16),
		instruments:     newCoordinatorInstruments(resolveMeter(meter)),
		instanceID:      uuid.New().String(),
		consumedOffsets: make(map[int32]int64),
	}
	k.offsetsCond = sync.NewCond(&k.mu)

	return k
}

// Errors returns a read-only channel that receives errors from background goroutines
// (e.g., the redirect consumer). The channel is buffered and never closed.
func (k *KafkaStateCoordinator) Errors() <-chan error {
	return k.errCh
}

// IsLocked checks if a message's key is currently locked in the local state.
// This method delegates to the underlying localStateCoordinator for fast, thread-safe lock check.
func (k *KafkaStateCoordinator) IsLocked(ctx context.Context, msg *InternalMessage) bool {
	return k.local.IsLocked(ctx, msg)
}

// Start initializes the coordinator for the given topic and begins background synchronization.
// This method performs three steps:
//
//	Ensures the compacted redirect topic exists (creates if needed)
//	Restores local state by consuming the redirect topic's history
//	Launches a background consumer to keep state synchronized
//
// This is a blocking call that restores full state before returning.
// Once it completes, the coordinator is ready to use with accurate lock state.
func (k *KafkaStateCoordinator) Start(ctx context.Context, topic string) error {
	k.mu.Lock()
	k.topic = topic

	// create a derived context for background workers that we can cancel
	workerCtx, cancel := context.WithCancel(ctx)
	k.cancel = cancel
	k.mu.Unlock()

	// ensure redirect topic exists
	if err := k.ensureRedirectTopic(ctx, topic); err != nil {
		return err
	}

	// restore state from redirect topic
	if err := k.restoreState(ctx, topic); err != nil {
		return err
	}

	// start monitoring of redirect topic
	return k.startRedirectConsumer(workerCtx, topic)
}

// Acquire locks the key for the given message.
// It uses optimistic locking: updates local state immediately, then publishes to the redirect topic.
// On publish failure, local state is rolled back and error returned.
func (k *KafkaStateCoordinator) Acquire(ctx context.Context, originalTopic string, msg *InternalMessage) error {
	id, ok := GetHeaderValue[string](msg.headerData, HeaderID)
	if !ok {
		// if no ID is provided (e.g., first failure), generate a unique one.
		// we MUST persist this to the message headers so that:
		// 1. the Retry Topic receives this ID.
		// 2. subsequent Release() calls use the same ID.
		id = uuid.New().String()
		_ = SetHeader(msg.headerData, HeaderID, id)
	}

	// Persist original topic to message headers for Release() to use
	if _, ok := GetHeaderValue[string](msg.headerData, HeaderTopic); !ok {
		_ = SetHeader(msg.headerData, HeaderTopic, originalTopic)
	}

	// optimistic locking: update local state immediately
	if err := k.local.Acquire(ctx, originalTopic, msg); err != nil {
		return err
	}

	// use a clone of the original headers to ensure we preserve any metadata
	// while adding coordinator-specific headers.
	redirectHeaders, _ := msg.headerData.Clone().(*HeaderList)
	redirectHeaders.Set(HeaderTopic, []byte(originalTopic))
	redirectHeaders.Set(HeaderKey, msg.key)
	redirectHeaders.Set(HeaderCoordinatorID, []byte(k.instanceID))

	// IMPORTANT: We use a unique UUID as the Kafka message key (not the original message key).
	// This is critical for handling multiple in-flight messages with the same original key.
	//
	// Example: 4 messages with key="order-123" all fail and enter the retry chain.
	// If we used the original key for the compacted redirect topic:
	//   - Compaction would merge all 4 lock records into 1
	//   - A single tombstone would "release" all locks prematurely
	//
	// With UUID-as-key, each lock is independent:
	//   - uuid1 → lock, uuid2 → lock, uuid3 → lock, uuid4 → lock
	//   - Each requires its own tombstone to release
	//   - Local state uses reference counting on original key (refcount=4)
	//   - Main topic unblocks only after ALL 4 retries complete (refcount=0)
	//
	// The original key is preserved in HeaderKey for local lock management.
	// payload value is non-nil to distinguish from tombstone (nil = release).
	redirectMsg := &InternalMessage{
		topic:      k.redirectTopic(originalTopic),
		key:        []byte(id),
		payload:    []byte(id),
		headerData: redirectHeaders,
		timestamp:  time.Now(),
	}

	err := retry.Do(
		func() error {
			return k.producer.Produce(ctx, redirectMsg.Topic(), redirectMsg)
		},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.MaxDelay(2*time.Second),
		retry.OnRetry(func(n uint, err error) {
			k.logger.Warn(
				"failed to produce acquire message",
				"topic",
				originalTopic,
				"attempt",
				n+1,
				"error",
				err,
			)
		}),
	)
	if err != nil {
		// rollback local state on failure
		_ = k.local.Release(ctx, msg)

		return fmt.Errorf("failed to produce acquire message: %w", err)
	}

	k.instruments.lockAcquired.Add(ctx, 1,
		metric.WithAttributes(attribute.String("topic", originalTopic)),
	)

	return nil
}

// Release unlocks the key for the given message.
// It publishes a tombstone (null payload) to the compacted redirect topic,
// which signals all coordinators to remove the lock.
// Uses optimistic locking with rollback on failure, similar to Acquire.
func (k *KafkaStateCoordinator) Release(ctx context.Context, msg *InternalMessage) error {
	id, ok := GetHeaderValue[string](msg.headerData, HeaderID)
	if !ok {
		return errors.New("HeaderID is required for Release: message was not properly acquired")
	}

	topic, ok := GetHeaderValue[string](msg.headerData, HeaderTopic)
	if !ok {
		return errors.New("HeaderTopic is required for Release: message was not properly acquired")
	}

	// optimistic release: update local state immediately
	if err := k.local.Release(ctx, msg); err != nil {
		return err
	}

	// use a clone of the original headers to ensure we preserve metadata (like HeaderID)
	tombstoneHeaders, _ := msg.headerData.Clone().(*HeaderList)
	tombstoneHeaders.Set(HeaderTopic, []byte(topic))
	tombstoneHeaders.Set(HeaderKey, msg.key)
	tombstoneHeaders.Set(HeaderCoordinatorID, []byte(k.instanceID))

	tombstoneHeaders.Set(HeaderID, []byte(id))

	tombstoneMsg := &InternalMessage{
		topic:      k.redirectTopic(topic),
		key:        []byte(id),
		payload:    nil, // tombstone
		headerData: tombstoneHeaders,
		timestamp:  time.Now(),
	}

	err := retry.Do(
		func() error {
			return k.producer.Produce(ctx, tombstoneMsg.Topic(), tombstoneMsg)
		},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.MaxDelay(2*time.Second),
		retry.OnRetry(func(n uint, err error) {
			k.logger.Warn(
				"failed to produce release message",
				"topic",
				topic,
				"attempt",
				n+1,
				"error",
				err,
			)
		}),
	)
	if err != nil {
		// rollback local state on failure
		_ = k.local.Acquire(ctx, topic, msg)

		return fmt.Errorf("failed to produce release message: %w", err)
	}

	k.instruments.lockReleased.Add(ctx, 1,
		metric.WithAttributes(attribute.String("topic", topic)),
	)

	return nil
}

// Synchronize ensures the coordinator's local state is up-to-date with the distributed source of truth.
// It blocks until the internal redirect consumer has reached the High Water Mark of the redirect topic.
func (k *KafkaStateCoordinator) Synchronize(ctx context.Context) error {
	k.mu.RLock()
	topic := k.topic
	k.mu.RUnlock()

	if topic == "" {
		return errors.New("coordinator not started: topic is empty")
	}

	redirectTopic := k.redirectTopic(topic)

	// get HWM for the redirect topic
	metadata, err := k.admin.DescribeTopics(ctx, []string{redirectTopic})
	if err != nil {
		return fmt.Errorf("failed to describe redirect topic for sync: %w", err)
	}

	if len(metadata) == 0 {
		return nil
	}

	targetOffsets := metadata[0].PartitionOffsets()

	// check if already caught up
	k.mu.RLock()

	if k.isCaughtUp(targetOffsets) {
		k.mu.RUnlock()

		return nil
	}

	k.mu.RUnlock()

	k.logger.Debug("waiting for redirect consumer to catch up", "topic", redirectTopic, "targets", targetOffsets)

	// default timeout
	timeout := 30 * time.Second
	if k.cfg.StateRestoreTimeoutMs > 0 {
		timeout = time.Duration(k.cfg.StateRestoreTimeoutMs) * time.Millisecond
	}

	deadline := time.Now().Add(timeout)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// wait for condition signal or timeout
	done := make(chan struct{})

	go func() {
		k.mu.Lock()
		defer k.mu.Unlock()

		for !k.isCaughtUp(targetOffsets) {
			if ctx.Err() != nil {
				return
			}

			k.offsetsCond.Wait()

			// check if we've exceeded the deadline
			if time.Now().After(deadline) {
				close(done)

				return
			}
		}

		close(done)
	}()

	select {
	case <-ctx.Done():
		k.offsetsCond.Broadcast()
		return ctx.Err()
	case <-timer.C:
		k.offsetsCond.Broadcast()

		k.mu.RLock()
		defer k.mu.RUnlock()

		return fmt.Errorf("timeout waiting for redirect consumer to catch up: consumed=%v, targets=%v", k.consumedOffsets, targetOffsets)
	case <-done:
		k.logger.Debug("redirect consumer caught up", "topic", redirectTopic)
		return nil
	}
}

// isCaughtUp checks if consumedOffsets have reached targetOffsets.
// Caller must hold k.mu (at least RLock).
func (k *KafkaStateCoordinator) isCaughtUp(targetOffsets map[int32]int64) bool {
	return isCaughtUpOffsets(k.consumedOffsets, targetOffsets)
}

// isCaughtUpOffsets checks if consumedOffsets have reached targetOffsets.
// target is HWM (next offset to be written), so we are caught up if we processed (target - 1).
func isCaughtUpOffsets(consumedOffsets, targetOffsets map[int32]int64) bool {
	for p, target := range targetOffsets {
		if target == 0 {
			// empty partition
			continue
		}

		consumed, ok := consumedOffsets[p]
		// target is HWM (next offset to be written)
		// we are caught up if we processed (target - 1)
		if !ok || consumed < target-1 {
			return false
		}
	}

	return true
}

// Close gracefully shuts down the coordinator by stopping background workers.
// The context controls the shutdown timeout - if it expires before workers exit,
// Close returns an error but the coordinator may have goroutines still running.
func (k *KafkaStateCoordinator) Close(ctx context.Context) error {
	k.mu.Lock()

	if k.cancel != nil {
		k.cancel()
	}

	k.mu.Unlock()

	// wait for workers with timeout
	done := make(chan struct{})

	go func() {
		k.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("coordinator shutdown timeout: %w", ctx.Err())
	}
}

// ensureRedirectTopic creates the compacted redirect topic if it doesn't exist.
// The redirect topic uses log compaction to retain only the latest lock state
// per key, and configures segment.ms=100 for fast tombstone propagation.
func (k *KafkaStateCoordinator) ensureRedirectTopic(ctx context.Context, topic string) error {
	redirectTopic := k.redirectTopic(topic)

	if k.cfg.DisableAutoTopicCreation {
		metadata, err := k.admin.DescribeTopics(ctx, []string{redirectTopic})
		if err != nil {
			return fmt.Errorf("failed to verify existence of redirect topic (auto-creation disabled): %w", err)
		}

		if !slices.ContainsFunc(metadata, func(m TopicMetadata) bool { return m.Name() == redirectTopic }) {
			return fmt.Errorf("required redirect topic '%s' missing (auto-creation disabled)", redirectTopic)
		}

		return nil
	}

	partitions := k.cfg.RetryTopicPartitions
	if partitions == 0 {
		metadata, err := k.admin.DescribeTopics(ctx, []string{topic})
		if err != nil {
			return fmt.Errorf("failed to describe primary topic '%s': %w", topic, err)
		}

		if len(metadata) > 0 {
			partitions = metadata[0].Partitions()
		}
	}

	// segment.ms=60000 (1 min) balances lock visibility with resource efficiency.
	// Kafka default is 7 days; 1 minute ensures tombstones propagate quickly enough
	// for lock releases while avoiding excessive segment rolling overhead.
	// Acceptable for retry systems where backoffs are typically seconds-to-minutes.
	return k.admin.CreateTopic(ctx, redirectTopic, partitions, k.cfg.ReplicationFactor, map[string]string{
		"cleanup.policy": "compact",
		"segment.ms":     "60000",
	})
}

// restoreState rebuilds local lock state by consuming the redirect topic's history.
// This is called once during Start() to synchronize with the distributed state before processing begins.
// Uses an ephemeral consumer group that is deleted after restoration completes.
// Times out if HWM is not reached within the configured window.
func (k *KafkaStateCoordinator) restoreState(ctx context.Context, topic string) error {
	start := time.Now()

	defer func() {
		k.instruments.stateRestoreDur.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(attribute.String("topic", topic)),
		)
	}()

	// get HWM for the redirect topic
	metadata, err := k.admin.DescribeTopics(ctx, []string{k.redirectTopic(topic)})
	if err != nil {
		return fmt.Errorf("failed to describe redirect topic for restore: %w", err)
	}

	if len(metadata) == 0 {
		k.logger.Debug("redirect topic metadata not found, skipping restore")
		return nil
	}

	targetOffsets := metadata[0].PartitionOffsets()

	hasData := slices.ContainsFunc(slices.Collect(maps.Values(targetOffsets)), func(o int64) bool {
		return o > 0
	})

	if !hasData {
		k.logger.Debug("redirect topic is empty, skipping restore")
		return nil
	}

	refillCfg := *k.cfg
	refillCfg.GroupID = uuid.New().String()

	refillConsumer, err := k.consumerFactory.NewConsumer(refillCfg.GroupID)
	if err != nil {
		return err
	}

	defer func() {
		// clean up ephemeral group
		go func() {
			if err := k.admin.DeleteConsumerGroup(context.Background(), refillCfg.GroupID); err != nil {
				k.logger.Warn("failed to delete ephemeral consumer group", "group", refillCfg.GroupID, "error", err)
			}
		}()
	}()

	// consume until we reach HWM
	// use a separate context for the refill loop that we cancel when done
	refillCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// safety timeout in case we never reach HWM (if HWM moves while consuming)
	// ideally, we should read until the "snapshot" of HWM we took at the start
	timeout := time.Duration(k.cfg.StateRestoreTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	refillCtx, cancelTimeout := context.WithTimeout(refillCtx, timeout)
	defer cancelTimeout()

	handler := &redirectFillHandler{
		k:               k,
		targetOffsets:   targetOffsets,
		consumedOffsets: make(map[int32]int64),
		cancel:          cancel,
	}

	err = refillConsumer.Consume(refillCtx, []string{k.redirectTopic(topic)}, handler)
	_ = refillConsumer.Close()

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("restore failed: %w", err)
	}

	return nil
}

// startRedirectConsumer launches a background goroutine that continuously consumes
// the redirect topic to keep local state synchronized with other coordinator instances.
// The consumer automatically restarts on errors with exponential backoff.
// Consumer group name: {groupID}-redirect.
func (k *KafkaStateCoordinator) startRedirectConsumer(ctx context.Context, topic string) error {
	redirectCfg := *k.cfg
	redirectCfg.GroupID = k.cfg.GroupID + "-redirect"

	consumer, err := k.consumerFactory.NewConsumer(redirectCfg.GroupID)
	if err != nil {
		return err
	}

	k.wg.Add(1)

	go func() {
		defer k.wg.Done()
		defer func() { _ = consumer.Close() }()

		backoff := 5 * time.Second

		for {
			err := consumer.Consume(ctx, []string{k.redirectTopic(topic)}, &redirectHandler{k: k})

			// check for clean shutdown
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}

			if err != nil {
				k.logger.Error("background redirect consumer failed, restarting",
					"error", err,
					"retry_in", backoff,
				)

				select {
				case k.errCh <- err:
				default:
					k.logger.Warn("error channel full, dropping background error", "error", err)
				}
			}
			// wait before restarting
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				// continue loop
			}
		}
	}()

	return nil
}

// processRedirectMessage handles incoming messages from the redirect topic.
// Tombstones (null payload) trigger lock release, while regular messages trigger acquisition.
// Ignores echo messages from the same coordinator instance to avoid redundant processing.
func (k *KafkaStateCoordinator) processRedirectMessage(ctx context.Context, msg Message) error {
	// ignore echo messages from self
	if originID, ok := msg.Headers().Get(HeaderCoordinatorID); ok {
		if string(originID) == k.instanceID {
			return nil
		}
	}

	if msg.Value() == nil {
		return k.releaseMessage(ctx, NewFromMessage(msg))
	}

	originalTopicBytes, ok := msg.Headers().Get(HeaderTopic)
	if !ok {
		return errors.New("redirect message missing topic header")
	}

	originalKeyBytes, ok := msg.Headers().Get(HeaderKey)
	if !ok {
		return errors.New("redirect message missing key header")
	}

	originalMsg := &InternalMessage{
		topic:      string(originalTopicBytes),
		key:        originalKeyBytes,
		headerData: &HeaderList{},
	}

	// delegate to local coordinator
	return k.local.Acquire(ctx, originalMsg.topic, originalMsg)
}

// releaseMessage releases a lock based on a received tombstone message.
// This is called when processing tombstones from other coordinator instances,
// ensuring all coordinators maintain consistent lock state. Requires topic and
// key headers to identify the lock to release.
func (k *KafkaStateCoordinator) releaseMessage(ctx context.Context, msg *InternalMessage) error {
	if _, ok := GetHeaderValue[string](msg.headerData, HeaderTopic); !ok {
		return errors.New("topic header not found")
	}

	if _, ok := GetHeaderValue[string](msg.headerData, HeaderKey); !ok {
		return errors.New("key header not found")
	}

	// delegate to local coordinator
	return k.local.Release(ctx, msg)
}

// redirectTopic returns the compacted topic name used for lock state.
// The naming convention follows the pattern: {prefix}_{original_topic}
func (k *KafkaStateCoordinator) redirectTopic(topic string) string {
	return k.cfg.RedirectTopicPrefix + "_" + topic
}

// redirectFillHandler is a one-time consumer handler used during state restoration.
// It tracks consumed offsets and cancels consumption once all partitions reach
// their target High Water Marks, ensuring complete state reconstruction.
type redirectFillHandler struct {
	k               *KafkaStateCoordinator
	targetOffsets   map[int32]int64
	consumedOffsets map[int32]int64
	cancel          context.CancelFunc
	mu              sync.Mutex
}

// Handle processes a redirect message during state restoration and tracks progress.
// Once all partitions reach their target offsets, it cancels the restoration consumer.
func (r *redirectFillHandler) Handle(ctx context.Context, msg Message) error {
	if err := r.k.processRedirectMessage(ctx, msg); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.consumedOffsets[msg.Partition()] = msg.Offset()

	// check if all partitions have reached their target offset
	if isCaughtUpOffsets(r.consumedOffsets, r.targetOffsets) {
		r.cancel()
	}

	return nil
}

// redirectHandler is the ongoing background consumer handler for the redirect topic.
// It keeps the coordinator's local state synchronized with other instances by
// processing lock acquisitions and releases from the distributed compacted topic.
type redirectHandler struct {
	k *KafkaStateCoordinator
}

// Handle processes a redirect message during normal operation and updates local state.
// It broadcasts offset updates to wake any goroutines waiting in Synchronize().
func (r *redirectHandler) Handle(ctx context.Context, msg Message) error {
	if err := r.k.processRedirectMessage(ctx, msg); err != nil {
		return err
	}

	r.k.mu.Lock()
	r.k.consumedOffsets[msg.Partition()] = msg.Offset()
	r.k.offsetsCond.Broadcast() // wake up any goroutines waiting in Synchronize
	r.k.mu.Unlock()

	return nil
}
