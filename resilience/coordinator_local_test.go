package resilience

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTopicChaos = "chaos-topic"

func TestLocalStateCoordinator_Acquire(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	msg := &InternalMessage{
		topic:      "orders",
		key:        []byte("order-123"),
		headerData: &HeaderList{},
	}

	err := coordinator.Acquire(ctx, "orders", msg)
	require.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_Release(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	msg := &InternalMessage{
		topic:      "orders",
		key:        []byte("order-123"),
		headerData: &HeaderList{},
	}

	// lock first
	_ = coordinator.Acquire(ctx, "orders", msg)
	assert.True(t, coordinator.IsLocked(ctx, msg))

	// release
	err := coordinator.Release(ctx, msg)
	require.NoError(t, err)

	assert.False(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_ReferenceCounting(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	topic := testTopicOrders
	key := "ref-key"
	msg := &InternalMessage{topic: topic, key: []byte(key), headerData: &HeaderList{}}

	// first Lock
	err := coordinator.Acquire(ctx, topic, msg)
	require.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 1, count)

	// second lock
	err = coordinator.Acquire(ctx, topic, msg)
	require.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ = coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 2, count)

	// first release
	err = coordinator.Release(ctx, msg)
	require.NoError(t, err)

	// should STILL be locked (Ref count 2 -> 1)
	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ = coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 1, count)

	// second release
	err = coordinator.Release(ctx, msg)
	require.NoError(t, err)

	// should be UNLOCKED (Ref count 1 -> 0)
	assert.False(t, coordinator.IsLocked(ctx, msg))
	count, exists := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 0, count)
	assert.False(t, exists)
}

func TestLocalStateCoordinator_Isolation(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	// lock key A
	msgA := &InternalMessage{topic: "topic1", key: []byte("keyA"), headerData: &HeaderList{}}
	_ = coordinator.Acquire(ctx, "topic1", msgA)

	// check key A is locked
	assert.True(t, coordinator.IsLocked(ctx, msgA))

	// check key B is NOT locked
	msgB := &InternalMessage{topic: "topic1", key: []byte("keyB"), headerData: &HeaderList{}}
	assert.False(t, coordinator.IsLocked(ctx, msgB))

	// check key A on different topic is NOT locked
	msgOtherTopic := &InternalMessage{topic: "topic2", key: []byte("keyA"), headerData: &HeaderList{}}
	assert.False(t, coordinator.IsLocked(ctx, msgOtherTopic))
}

func TestLocalStateCoordinator_Release_WithHeader(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	// acquire on "orders"
	msg := &InternalMessage{topic: "orders", key: []byte("key1"), headerData: &HeaderList{}}
	_ = coordinator.Acquire(ctx, "orders", msg)

	// release using a message with explicit headerTopic (simulating redirect message)
	releaseMsg := &InternalMessage{
		topic:      "redirect_orders", // different topic
		key:        []byte("key1"),
		headerData: &HeaderList{},
	}
	releaseMsg.headerData.Set(headerTopic, []byte("orders"))

	err := coordinator.Release(ctx, releaseMsg)
	require.NoError(t, err)

	assert.False(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var (
		ctx         = t.Context()
		coordinator = newLocalStateCoordinator()
		topic       = "concurrent-topic"
		key         = "concurrent-key"
		msg         = &InternalMessage{topic: topic, key: []byte(key), headerData: &HeaderList{}}
		concurrency = 100
		iterations  = 100

		wg sync.WaitGroup
	)

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				_ = coordinator.Acquire(ctx, topic, msg)

				if !coordinator.IsLocked(ctx, msg) {
					t.Error("should be locked")
				}

				_ = coordinator.Release(ctx, msg)
			}
		}()
	}

	wg.Wait()

	// after all goroutines finish, ref count should be 0
	assert.False(t, coordinator.IsLocked(ctx, msg))
	count, exists := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 0, count)
	assert.False(t, exists)
}

func TestLocalStateCoordinator_Chaos(t *testing.T) {
	t.Parallel()

	coordinator := newLocalStateCoordinator()
	ctx := t.Context()

	numGoroutines := 50
	operationsPerGoroutine := 100
	numKeys := 10 // small key space to force collisions

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// simulate random Acquire/Release operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				keyID := (routineID + j) % numKeys
				keyStr := string([]byte{byte(keyID)}) // "0", "1", ...
				topic := testTopicChaos

				msg := &InternalMessage{topic: topic, key: []byte(keyStr), headerData: &HeaderList{}}

				_ = coordinator.Acquire(ctx, topic, msg)

				if !coordinator.IsLocked(ctx, msg) {
					t.Errorf("routine %d: key %d should be locked", routineID, keyID)
				}

				_ = coordinator.Release(ctx, msg)
			}
		}(i)
	}

	wg.Wait()

	// verify all keys are unlocked and map is clean
	for k := 0; k < numKeys; k++ {
		keyStr := string([]byte{byte(k)})
		topic := testTopicChaos
		count, exists := coordinator.lm.getRefCount(topic, keyStr)

		assert.False(t, exists, "key %d should not exist in map", k)
		assert.Equal(t, 0, count, "key %d ref count should be 0", k)
	}
}
