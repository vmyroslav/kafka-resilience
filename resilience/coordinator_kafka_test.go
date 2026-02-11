package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaStateCoordinator_Acquire(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockAdmin := &AdminMock{}
	mockFactory := &ConsumerFactoryMock{}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...any) {},
		ErrorFunc: func(_ string, _ ...any) {},
	}

	cfg := &Config{
		RedirectTopicPrefix: "redirect",
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		mockLogger,
		mockProducer,
		mockFactory,
		mockAdmin,
		nil,
	)

	ctx := t.Context()
	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-123"),
		headerData: &headerList{},
	}
	msg.headerData.Set("custom", []byte("val"))

	err := coordinator.Acquire(ctx, "orders", msg)
	require.NoError(t, err)

	// optimistic locking: should be locked immediately
	assert.True(t, coordinator.IsLocked(ctx, msg))

	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	assert.Equal(t, "redirect_orders", call.Topic)

	headers := call.Msg.Headers().All()
	assert.Contains(t, headers, "key")
	assert.Equal(t, []byte("order-123"), headers["key"])
	assert.Contains(t, headers, headerTopic)
	assert.Equal(t, []byte("orders"), headers[headerTopic])
	assert.Contains(t, headers, headerCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[headerCoordinatorID])
}

func TestKafkaStateCoordinator_Release(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...any) {},
	}

	cfg := &Config{
		RedirectTopicPrefix: "redirect",
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		mockLogger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	ctx := t.Context()
	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-123"),
		headerData: &headerList{},
	}

	// Acquire first (sets up required headers: HeaderID, headerTopic)
	err := coordinator.Acquire(ctx, "orders", msg)
	require.NoError(t, err)
	assert.True(t, coordinator.IsLocked(ctx, msg))

	err = coordinator.Release(ctx, msg)
	require.NoError(t, err)

	// optimistic release: should be unlocked immediately
	assert.False(t, coordinator.IsLocked(ctx, msg))

	// 2 produce calls: 1 for acquire, 1 for release (tombstone)
	assert.Len(t, mockProducer.ProduceCalls(), 2)
	releaseCall := mockProducer.ProduceCalls()[1]

	headers := releaseCall.Msg.Headers().All()
	assert.Contains(t, headers, headerCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[headerCoordinatorID])
	// verify tombstone
	assert.Nil(t, releaseCall.Msg.Value())
}

func TestKafkaStateCoordinator_Start_RestoresState(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10},
				},
			}, nil
		},
		CreateTopicFunc: func(_ context.Context, _ string, _ int32, _ int16, _ map[string]string) error {
			return nil
		},
		DeleteConsumerGroupFunc: func(_ context.Context, _ string) error {
			return nil
		},
	}

	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, handler ConsumerHandler) error {
			// simulate reading a lock message from redirect topic
			// NO CoordinatorID header -> simulates legacy message or other instance
			msg := &MessageEnvelope{
				topic:      "redirect_orders",
				key:        []byte("order-locked"),
				payload:    []byte("order-locked"),
				headerData: &headerList{},
			}
			msg.headerData.Set("topic", []byte("orders"))
			msg.headerData.Set("key", []byte("order-locked"))

			msg.SetPartition(0)
			msg.SetOffset(9) // last offset (target is 10)
			_ = handler.Handle(ctx, msg)

			return nil
		},
		CloseFunc: func() error { return nil },
	}

	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, _ ConsumerHandler) error {
			<-ctx.Done()
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{}
	callCount := 0
	mockFactory.NewConsumerFunc = func(_ string) (Consumer, error) {
		callCount++
		if callCount == 1 {
			return restoreConsumer, nil
		}

		return liveConsumer, nil
	}

	cfg := &Config{
		RedirectTopicPrefix: "redirect",
		StateRestoreTimeout: 100 * time.Millisecond,
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		&LoggerMock{DebugFunc: func(_ string, _ ...any) {}, InfoFunc: func(_ string, _ ...any) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		nil,
	)

	err := coordinator.Start(t.Context(), "orders")
	require.NoError(t, err)

	// verify state was restored
	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-locked"),
		headerData: &headerList{},
	}
	assert.True(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_Acquire_ProducerError(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka error")
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			WarnFunc: func(_ string, _ ...any) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	err := coordinator.Acquire(t.Context(), "t", &MessageEnvelope{topic: "t", key: []byte("k"), headerData: &headerList{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka error")

	// verify rollback: should NOT be locked
	msg := &MessageEnvelope{topic: "t", key: []byte("k"), headerData: &headerList{}}
	assert.False(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_ProcessRedirect_Filter(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// simulate "Echo" message (Same ID)
	echoMsg := &MessageEnvelope{
		topic:      "redirect_orders",
		key:        []byte("k1"),
		payload:    []byte("k1"),
		headerData: &headerList{},
	}
	echoMsg.headerData.Set(headerCoordinatorID, []byte(coordinator.instanceID))
	echoMsg.headerData.Set(headerTopic, []byte("orders"))
	echoMsg.headerData.Set("key", []byte("k1"))

	// we call Acquire first to set local ref count to 1 (simulating the source of the echo)
	_ = coordinator.local.Acquire(t.Context(), "orders", &MessageEnvelope{topic: "orders", key: []byte("k1"), headerData: &headerList{}})

	// process the echo message
	err := coordinator.processRedirectMessage(t.Context(), echoMsg)
	require.NoError(t, err)

	// ref count should still be 1 (not incremented to 2)
	count, _ := coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 1, count)

	// simulate "foreign" message (different ID)
	foreignMsg := &MessageEnvelope{
		topic:      "redirect_orders",
		key:        []byte("k1"),
		payload:    []byte("k1"),
		headerData: &headerList{},
	}
	foreignMsg.headerData.Set(headerCoordinatorID, []byte("other-uuid"))
	foreignMsg.headerData.Set(headerTopic, []byte("orders"))
	foreignMsg.headerData.Set("key", []byte("k1"))

	err = coordinator.processRedirectMessage(t.Context(), foreignMsg)
	require.NoError(t, err)

	// ref count should now be 2
	count, _ = coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 2, count)
}

func TestKafkaStateCoordinator_ForeignTombstone(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// simulate we have a lock locally (maybe restored or acquired)
	_ = coordinator.local.Acquire(t.Context(), "orders", &MessageEnvelope{topic: "orders", key: []byte("key1"), headerData: &headerList{}})
	msg := &MessageEnvelope{topic: "orders", key: []byte("key1"), headerData: &headerList{}}
	assert.True(t, coordinator.IsLocked(t.Context(), msg))

	// receive a Tombstone from a different coordinator (failover scenario)
	tombstone := createMockRedirectMsg("orders", "key1", "other-instance", false)

	err := coordinator.processRedirectMessage(t.Context(), tombstone)
	require.NoError(t, err)

	// should be unlocked
	assert.False(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_Rebalance_Simulation(t *testing.T) {
	// Scenario:
	// 1. Instance A locks "order-1" (persisted to Redirect Topic).
	// 2. Instance A crashes (or rebalance happens).
	// 3. Instance B starts up, assigned the same partition.
	// 4. Instance B consumes the Redirect Topic and must restore the lock locally.
	topic := testTopicOrders
	key := "order-1"

	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 5}, // HWM is 5
				},
			}, nil
		},
		CreateTopicFunc: func(_ context.Context, _ string, _ int32, _ int16, _ map[string]string) error {
			return nil
		},
		DeleteConsumerGroupFunc: func(_ context.Context, _ string) error {
			return nil
		},
	}

	// this consumer will be used by restoreState
	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, handler ConsumerHandler) error {
			// simulate the existing lock message on the topic
			msg := &MessageEnvelope{
				topic:      "redirect_orders",
				key:        []byte(key),
				payload:    []byte(key), // payload exists = Locked
				headerData: &headerList{},
			}
			msg.headerData.Set(headerTopic, []byte(topic))
			msg.headerData.Set(headerKey, []byte(key))
			// Note: Different coordinator ID, simulating Instance A
			msg.headerData.Set(headerCoordinatorID, []byte("instance-A"))

			msg.SetPartition(0)
			msg.SetOffset(4) // < HWM (5)

			if err := handler.Handle(ctx, msg); err != nil {
				return err
			}

			return nil
		},
		CloseFunc: func() error { return nil },
	}

	// live consumer (post-restore)
	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, _ ConsumerHandler) error {
			<-ctx.Done()
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{
		NewConsumerFunc: func(_ string) (Consumer, error) {
			// First call is for restore
			return restoreConsumer, nil
		},
	}
	// we need to patch the factory for the second call (live consumer) inside the test flow or use a counter
	callCount := 0
	mockFactory.NewConsumerFunc = func(_ string) (Consumer, error) {
		callCount++
		if callCount == 1 {
			return restoreConsumer, nil
		}

		return liveConsumer, nil
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect", StateRestoreTimeout: 100 * time.Millisecond},
		&LoggerMock{DebugFunc: func(_ string, _ ...any) {}, InfoFunc: func(_ string, _ ...any) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		nil,
	)

	// start Instance B
	err := coordinator.Start(t.Context(), topic)
	require.NoError(t, err)

	// verify Instance B has the lock
	checkMsg := &MessageEnvelope{topic: topic, key: []byte(key), headerData: &headerList{}}
	assert.True(t, coordinator.IsLocked(t.Context(), checkMsg), "Instance B should have restored the lock")
}

const testTopicOrders = "orders"

func TestKafkaStateCoordinator_Synchronize_TopicNotStarted(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// should fail because Start() wasn't called (topic is empty)
	err := coordinator.Synchronize(t.Context())
	require.ErrorContains(t, err, "coordinator not started")
}

func TestKafkaStateCoordinator_Synchronize_EmptyRedirectTopic(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			// return metadata indicating empty topic
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{}, // no offsets
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	// manually set topic as if Start() was called
	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.mu.Unlock()

	// should return immediately
	err := coordinator.Synchronize(t.Context())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_AlreadyCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	// simulate that we have already consumed up to offset 9 (HWM 10 means next is 10, so 9 is the last one)
	coordinator.consumedOffsets[0] = 9
	coordinator.mu.Unlock()

	err := coordinator.Synchronize(t.Context())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_BlocksUntilCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			DebugFunc: func(_ string, _ ...any) {
				// no-op
			},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.consumedOffsets[0] = 5 // lagging behind (5 < 9)
	coordinator.mu.Unlock()

	// use a channel to signal when Synchronize returns
	done := make(chan error)

	go func() {
		done <- coordinator.Synchronize(t.Context())
	}()

	// ensure it blocks initially
	select {
	case <-done:
		t.Fatal("Synchronize should have blocked")
	case <-time.After(100 * time.Millisecond):
		// expected behavior
	}

	// simulate background consumer updating the offset
	coordinator.mu.Lock()
	coordinator.consumedOffsets[0] = 9  // catch up
	coordinator.offsetsCond.Broadcast() // signal that offsets updated
	coordinator.mu.Unlock()

	// now it should complete
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Synchronize failed to return after catching up")
	}
}

func TestKafkaStateCoordinator_Synchronize_ContextCancellation(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 100}, // HWM = 100
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			DebugFunc: func(_ string, _ ...any) {
				// no-op
			},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.consumedOffsets[0] = 5 // lagging
	coordinator.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := coordinator.Synchronize(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	// let it block for a bit
	time.Sleep(50 * time.Millisecond)
	cancel()

	wg.Wait()
}

// Helper to create mock messages
func createMockRedirectMsg(topic, key, coordinatorID string, isLock bool) *MessageEnvelope {
	var value []byte
	if isLock {
		value = []byte(key)
	} else {
		value = nil
	}

	hl := &headerList{}
	hl.Set(headerCoordinatorID, []byte(coordinatorID))
	hl.Set(headerTopic, []byte(topic))
	hl.Set("key", []byte(key))

	return &MessageEnvelope{
		topic:      "redirect_" + topic,
		key:        []byte(key),
		payload:    value,
		headerData: hl,
	}
}

type mockTopicMetadata struct {
	name       string
	partitions int32
	offsets    map[int32]int64
}

func (m *mockTopicMetadata) Name() string                      { return m.name }
func (m *mockTopicMetadata) Partitions() int32                 { return m.partitions }
func (m *mockTopicMetadata) PartitionOffsets() map[int32]int64 { return m.offsets }
