//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// TestIntegration_KafkaCoordinator verifies that the KafkaStateCoordinator correctly
// interacts with a real Kafka cluster to acquire and release locks.
// This is a WHITE-BOX test that inspects the underlying Kafka topics.
func TestIntegration_KafkaCoordinator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("coord-test")
	redirectTopicName := "redirect_" + topic

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.RetryTopicPartitions = 1

	coord := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	// start Coordinator
	err := coord.Start(ctx, topic)
	require.NoError(t, err, "Coordinator should start and create topics successfully")

	// verify that Start() actually created the redirect topic with correct config
	// metadata propagation in Kafka can be slow on CI, so poll until visible
	var metadata []resilience.TopicMetadata
	require.Eventually(t, func() bool {
		var descErr error
		metadata, descErr = adapters.Admin.DescribeTopics(ctx, []string{redirectTopicName})
		return descErr == nil && len(metadata) == int(cfg.RetryTopicPartitions)
	}, 10*time.Second, 500*time.Millisecond, "redirect topic should become visible in metadata")
	assert.Equal(t, redirectTopicName, metadata[0].Name())
	assert.Equal(
		t,
		int32(1),
		metadata[0].Partitions(),
		"topic should be created with %d partitions",
		cfg.RetryTopicPartitions,
	)

	// create a separate consumer to spy on the redirect topic and verify messages
	spyConsumer, err := sarama.NewConsumerFromClient(client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spyConsumer.Close() })

	partitionConsumer, err := spyConsumer.ConsumePartition(redirectTopicName, 0, sarama.OffsetOldest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = partitionConsumer.Close() })

	// test Acquire Locking
	msgKey := []byte("test-lock-key")
	msg := resilience.NewInternalMessage(topic, msgKey, []byte("payload"), nil)

	t.Log("Acquiring lock...")
	err = coord.Acquire(ctx, topic, msg)
	require.NoError(t, err, "Should acquire lock successfully")

	// verify locally
	assert.True(t, coord.IsLocked(ctx, msg), "IsLocked should return true immediately")

	// verify that Acquire() actually produced a Lock message to the redirect topic
	var capturedUUID string
	select {
	case consumedMsg := <-partitionConsumer.Messages():
		// verify Key is a UUID
		capturedUUID = string(consumedMsg.Key)
		assert.NotEqual(t, string(msgKey), capturedUUID, "Kafka Key should be a UUID, not the resource key")
		_, err := uuid.Parse(capturedUUID)
		assert.NoError(t, err, "Kafka Key should be a valid UUID")

		// verify Value (lock contains the Key/ID as value)
		assert.Equal(t, capturedUUID, string(consumedMsg.Value), "Lock message value should match the generated ID")
		// verify Headers
		headers := toMap(consumedMsg.Headers)
		assert.Equal(t, topic, string(headers["topic"]), "Header 'topic' should match")
		assert.Equal(t, string(msgKey), string(headers["key"]), "Header 'key' should match original resource key")
		assert.Equal(t, capturedUUID, string(headers["id"]), "Header 'id' should match the generated UUID")
		assert.Contains(t, headers, "x-coordinator-id", "Header 'x-coordinator-id' should be present")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for Lock message on redirect topic")
	}

	// test Release
	err = coord.Release(ctx, msg)
	require.NoError(t, err, "Should release lock successfully")

	// verify locally
	assert.False(t, coord.IsLocked(ctx, msg), "IsLocked should return false immediately")

	// verify that Release() actually produced a Tombstone
	select {
	case consumedMsg := <-partitionConsumer.Messages():
		t.Log("Spy consumer received tombstone")
		// verify Key matches the UUID we captured during Acquire
		assert.Equal(t, capturedUUID, string(consumedMsg.Key), "Tombstone key should match the UUID used for locking")
		// verify Value (Tombstone must be nil or empty)
		assert.Nil(t, consumedMsg.Value, "Tombstone value should be nil")
		// verify Headers
		headers := toMap(consumedMsg.Headers)
		assert.Contains(t, headers, "x-coordinator-id", "Header 'x-coordinator-id' should be present")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for Tombstone on redirect topic")
	}
}

// TestIntegration_KafkaCoordinator_ForeignLock verifies that the coordinator
// correctly respects locks created by OTHER instances (foreign locks).
func TestIntegration_KafkaCoordinator_ForeignLock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("coord-foreign")
	redirectTopic := "redirect_" + topic

	// use raw Sarama producer to simulate a "Foreign" coordinator
	foreignProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = foreignProducer.Close() })

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.RedirectTopicPrefix = "redirect"
	cfg.RetryTopicPartitions = 1

	coord := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	err = coord.Start(ctx, topic)
	require.NoError(t, err)

	key := "shared-key"
	msg := resilience.NewInternalMessage(topic, []byte(key), nil, nil)

	// initially NOT locked
	assert.False(t, coord.IsLocked(ctx, msg))

	// simulate FOREIGN ACQUIRE
	// produce a message to redirect topic with a DIFFERENT Coordinator ID
	foreignID := uuid.New().String()
	foreignLockMsg := &sarama.ProducerMessage{
		Topic: redirectTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(key), // Lock value
		Headers: []sarama.RecordHeader{
			{Key: []byte("topic"), Value: []byte(topic)},
			{Key: []byte("key"), Value: []byte(key)},
			{Key: []byte("x-coordinator-id"), Value: []byte(foreignID)},
		},
	}

	_, _, err = foreignProducer.SendMessage(foreignLockMsg)
	require.NoError(t, err)

	// verify Local Coordinator eventually sees the lock
	assert.Eventually(t, func() bool {
		return coord.IsLocked(ctx, msg)
	}, 10*time.Second, 100*time.Millisecond, "coordinator should detect foreign lock")

	// simulate FOREIGN RELEASE (Tombstone)
	foreignTombstone := &sarama.ProducerMessage{
		Topic: redirectTopic,
		Key:   sarama.StringEncoder(key),
		Value: nil, // Tombstone
		Headers: []sarama.RecordHeader{
			{Key: []byte("topic"), Value: []byte(topic)},
			{Key: []byte("key"), Value: []byte(key)},
			{Key: []byte("x-coordinator-id"), Value: []byte(foreignID)},
		},
	}

	_, _, err = foreignProducer.SendMessage(foreignTombstone)
	require.NoError(t, err)

	// verify Local Coordinator eventually sees the release
	assert.Eventually(t, func() bool {
		return !coord.IsLocked(ctx, msg)
	}, 10*time.Second, 100*time.Millisecond, "coordinator should detect foreign unlock")
}

// TestIntegration_KafkaCoordinator_Rebalance verifies that a NEW coordinator instance
// successfully restores lock state from the redirect topic upon startup.
func TestIntegration_KafkaCoordinator_Rebalance(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	topic, groupID := newTestIDs("coord-rebal")
	key := "persistent-key"

	// create "Instance A"
	adaptersA := newTestAdapters(t, client)

	cfgA := resilience.NewDefaultConfig()
	cfgA.GroupID = groupID
	cfgA.RetryTopicPartitions = 1
	cfgA.StateRestoreIdleTimeoutMs = 1000

	coordA := resilience.NewKafkaStateCoordinator(
		cfgA, sharedLogger.With("component", "InstanceA"),
		adaptersA.Producer, adaptersA.ConsumerFactory, adaptersA.Admin,
		nil,
	)

	// Start Instance A
	ctxA, cancelA := context.WithCancel(ctx)
	err := coordA.Start(ctxA, topic)
	require.NoError(t, err)

	// instance A acquires lock
	msg := resilience.NewInternalMessage(topic, []byte(key), nil, nil)

	err = coordA.Acquire(ctxA, topic, msg)
	require.NoError(t, err)
	require.True(t, coordA.IsLocked(ctxA, msg))

	// wait a bit for the lock message to be definitely persisted and index updated
	// in a real scenario, Acquire waits for producer ack, so it should be there.
	time.Sleep(2 * time.Second)

	// "Stop" Instance A
	cancelA()

	// create "Instance B" (Same Group ID)
	// this simulates a new process starting up after the first one died
	adaptersB := newTestAdapters(t, client)

	cfgB := resilience.NewDefaultConfig()
	cfgB.GroupID = groupID // Same Group ID
	cfgB.RetryTopicPartitions = 1
	cfgB.StateRestoreIdleTimeoutMs = 1000

	coordB := resilience.NewKafkaStateCoordinator(
		cfgB, sharedLogger.With("component", "InstanceB"),
		adaptersB.Producer, adaptersB.ConsumerFactory, adaptersB.Admin,
		nil,
	)

	// start Instance B
	// ut should read the Redirect topic during Start() and populate its local map
	err = coordB.Start(ctx, topic)
	require.NoError(t, err)

	// verify Instance B sees the lock
	msgB := resilience.NewInternalMessage(topic, []byte(key), nil, nil)

	isLocked := coordB.IsLocked(ctx, msgB)
	assert.True(t, isLocked, "instance B should have restored the lock state from Kafka")
}

// Helper to convert headers
func toMap(headers []*sarama.RecordHeader) map[string][]byte {
	m := make(map[string][]byte)
	for _, h := range headers {
		m[string(h.Key)] = h.Value
	}
	return m
}

// TestIntegration_KafkaCoordinator_Synchronize verifies that Synchronize correctly
// blocks until the local state reflects the remote state.
func TestIntegration_KafkaCoordinator_Synchronize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("coord-sync")
	redirectTopic := "redirect_" + topic

	// create coordinator
	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.RetryTopicPartitions = 1
	cfg.RedirectTopicPrefix = "redirect"

	coord := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)

	// start Coordinator (creates topic)
	err := coord.Start(ctx, topic)
	require.NoError(t, err)

	// produce message to Redirect Topic *before* Synchronize
	// simulates existing lag during rebalancing
	externalProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err)
	defer externalProducer.Close()

	key := "sync-key"
	_, _, err = externalProducer.SendMessage(&sarama.ProducerMessage{
		Topic: redirectTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(key),
		Headers: []sarama.RecordHeader{
			{Key: []byte("topic"), Value: []byte(topic)},
			{Key: []byte("key"), Value: []byte(key)},
			{Key: []byte("x-coordinator-id"), Value: []byte("external")},
		},
	})
	require.NoError(t, err)

	// call Synchronize - should block until message above is consumed
	err = coord.Synchronize(ctx)
	require.NoError(t, err)

	// verify lock is visible
	msg := resilience.NewInternalMessage(topic, []byte(key), nil, nil)
	assert.True(t, coord.IsLocked(ctx, msg), "lock should be visible after Synchronize")
}

// TestIntegration_RestartRestoration verifies that the ErrorTracker correctly restores
// state from the redirect topic after a full application restart.
// It tests the sequence: Lock A, Lock B, Unlock A -> Restart -> Expect B locked, A unlocked.
func TestIntegration_RestartRestoration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	topic, groupID := newTestIDs("test-restart")

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.StateRestoreTimeoutMs = 15000
	cfg.StateRestoreIdleTimeoutMs = 6000

	// -------------------------------------------------------------------------
	// PHASE 1: Populate State (Application Instance 1)
	// -------------------------------------------------------------------------
	// create Client 1 (manually managed for restart simulation)
	client1, err := sarama.NewClient([]string{sharedBroker}, newTestSaramaConfig())
	require.NoError(t, err)

	producer1, err := sarama.NewSyncProducerFromClient(client1)
	require.NoError(t, err)

	admin1, err := saramaadapter.NewAdminAdapter(client1)
	require.NoError(t, err)

	coord1 := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger,
		saramaadapter.NewProducerAdapter(producer1),
		saramaadapter.NewConsumerFactory(client1),
		admin1,
		nil,
	)

	tracker1, err := resilience.NewErrorTracker(
		cfg, sharedLogger,
		saramaadapter.NewProducerAdapter(producer1),
		saramaadapter.NewConsumerFactory(client1),
		admin1, coord1, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker1.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	keyLocked := "key-locked"
	keyUnlocked := "key-unlocked"

	msgLocked := saramaadapter.NewMessage(&sarama.ConsumerMessage{
		Topic: topic,
		Key:   []byte(keyLocked),
		Value: []byte("val-locked"),
	})
	msgUnlocked := saramaadapter.NewMessage(&sarama.ConsumerMessage{
		Topic: topic,
		Key:   []byte(keyUnlocked),
		Value: []byte("val-unlocked"),
	})

	// lock both
	err = tracker1.Redirect(ctx, msgLocked, fmt.Errorf("error 1"))
	require.NoError(t, err)
	err = tracker1.Redirect(ctx, msgUnlocked, fmt.Errorf("error 2"))
	require.NoError(t, err)

	// wait for locks to be established in tracker1
	assert.Eventually(t, func() bool {
		return tracker1.IsInRetryChain(ctx, msgLocked) && tracker1.IsInRetryChain(ctx, msgUnlocked)
	}, 10*time.Second, 500*time.Millisecond, "both keys should be locked in tracker1")

	// unlock one
	// simulate message coming from retry topic for Free
	retryMsgUnlock := &sarama.ConsumerMessage{
		Topic: tracker1.RetryTopic(topic),
		Key:   []byte(keyUnlocked),
		Value: []byte("val-unlocked"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("id"), Value: []byte(keyUnlocked)},
			{Key: []byte("retry"), Value: []byte("true")},
			{Key: []byte("topic"), Value: []byte(topic)},
		},
	}

	err = tracker1.Free(ctx, saramaadapter.NewMessage(retryMsgUnlock))
	require.NoError(t, err)

	// wait for unlock in tracker1
	assert.Eventually(t, func() bool {
		return !tracker1.IsInRetryChain(ctx, msgUnlocked)
	}, 10*time.Second, 500*time.Millisecond, "Key should be unlocked in tracker1")

	// shutdown Instance 1
	_ = producer1.Close()
	_ = client1.Close()

	// -------------------------------------------------------------------------
	// PHASE 2: Restart (Application Instance 2)
	// -------------------------------------------------------------------------
	client2 := newTestClient(t)
	adapters2 := newTestAdapters(t, client2)

	coord2 := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters2.Producer, adapters2.ConsumerFactory, adapters2.Admin,
		nil,
	)

	tracker2, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters2.Producer, adapters2.ConsumerFactory, adapters2.Admin,
		coord2, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker2.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	// -------------------------------------------------------------------------
	// PHASE 3: Verification
	// -------------------------------------------------------------------------
	// we verify using messages with the ORIGINAL topic
	verifyMsgLocked := saramaadapter.NewMessage(&sarama.ConsumerMessage{Topic: topic, Key: []byte(keyLocked)})
	verifyMsgUnlocked := saramaadapter.NewMessage(&sarama.ConsumerMessage{Topic: topic, Key: []byte(keyUnlocked)})

	// verify Locked Key is STILL Locked
	isLocked := tracker2.IsInRetryChain(ctx, verifyMsgLocked)
	assert.True(t, isLocked, "Key '%s' should be locked after restart", keyLocked)

	// verify Unlocked Key is FREE
	isUnlocked := !tracker2.IsInRetryChain(ctx, verifyMsgUnlocked)
	assert.True(t, isUnlocked, "Key '%s' should be free after restart (tombstone processed)", keyUnlocked)

	assert.True(t, isLocked, "Key '%s' should be locked", keyLocked)
	assert.True(t, isUnlocked, "Key '%s' should be unlocked", keyUnlocked)
}

// TestIntegration_TombstoneRestoration verifies that tombstones in the redirect topic
// are correctly handled during state restoration.
func TestIntegration_TombstoneRestoration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("tombstone")

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.StateRestoreTimeoutMs = 15000
	cfg.StateRestoreIdleTimeoutMs = 6000

	// create first tracker and start tracking
	coord1 := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)
	tracker1, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coord1, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	err = tracker1.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	// redirect message (locks the key)
	msg := &sarama.ConsumerMessage{
		Topic: topic,
		Key:   []byte("key-to-be-freed"),
		Value: []byte("value"),
	}
	err = tracker1.Redirect(ctx, saramaadapter.NewMessage(msg), fmt.Errorf("initial error"))
	require.NoError(t, err)

	// free message (writes tombstone to redirect topic)
	retryTopicMsg := &sarama.ConsumerMessage{
		Topic: tracker1.RetryTopic(topic),
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []*sarama.RecordHeader{
			{Key: []byte("id"), Value: msg.Key},
			{Key: []byte("retry"), Value: []byte("true")},
			{Key: []byte("topic"), Value: []byte(topic)},
		},
	}
	err = tracker1.Free(ctx, saramaadapter.NewMessage(retryTopicMsg))
	require.NoError(t, err)

	// create second tracker and restore state
	coord2 := resilience.NewKafkaStateCoordinator(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		nil,
	)
	tracker2, err := resilience.NewErrorTracker(
		cfg, sharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coord2, resilience.NewExponentialBackoff(),
		nil,
	)
	require.NoError(t, err)

	sharedLogger.Info("Starting tracker2 to verify restoration")
	err = tracker2.StartCoordinator(ctx, topic)
	require.NoError(t, err)

	// verify tombstone was correctly processed
	checkMsg := saramaadapter.NewMessage(&sarama.ConsumerMessage{Topic: topic, Key: msg.Key})
	isInChain := tracker2.IsInRetryChain(ctx, checkMsg)

	assert.False(t, isInChain, "message should NOT be in retry chain after restoration")
}
