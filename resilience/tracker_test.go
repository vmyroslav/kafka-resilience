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

type mockBackoff struct{}

func (m *mockBackoff) NextDelay(_ int) time.Duration {
	return time.Millisecond
}

// newTestConfig creates a valid config for tests
func newTestConfig() *Config {
	cfg := NewDefaultConfig()
	cfg.GroupID = testGroupID

	return cfg
}

func TestErrorTracker_Redirect_Rollback(t *testing.T) {
	// 1. Setup
	// Producer fails on Produce (simulating write failure to Retry Topic)
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Coordinator mocks: Acquire succeeds, Release MUST be called
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	// 2. Execute Redirect
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	// 3. Verify Result
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	// 4. Verify Rollback (Release called)
	assert.Len(t, mockCoordinator.ReleaseCalls(), 1, "Release should be called to rollback lock")
	assert.Len(t, mockCoordinator.AcquireCalls(), 1, "Acquire should have been called first")
}

func TestErrorTracker_Redirect_RollbackFailure_LogsCriticalError(t *testing.T) {
	// Test zombie key scenario: producer fails AND rollback fails
	// This is a critical error that should be logged
	var (
		criticalErrorLogged bool
		loggedMessage       string
	)

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Coordinator: Acquire succeeds, but Release ALSO fails (worst case)
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			return errors.New("release failed - kafka unavailable")
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
		WarnFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(msg string, _ ...interface{}) {
			if msg == "CRITICAL: failed to rollback lock after retry publish failure. Key may be permanently locked!" {
				criticalErrorLogged = true
				loggedMessage = msg
			}
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("zombie-key"),
		headerData: &headerList{},
	}

	// Execute Redirect - both produce and rollback will fail
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	// Should return the original produce error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	// Critical error should be logged about potential zombie key
	assert.True(t, criticalErrorLogged, "Critical error should be logged when rollback fails. Got: %s", loggedMessage)

	// Verify both Acquire and Release were attempted
	assert.Len(t, mockCoordinator.AcquireCalls(), 1)
	assert.GreaterOrEqual(t, len(mockCoordinator.ReleaseCalls()), 1, "Release should be attempted for rollback")
}

func TestErrorTracker_Redirect_RollbackSuccess_KeyNotLocked(t *testing.T) {
	// After successful rollback, the key should NOT be locked
	// This verifies the compensating transaction works correctly
	lockState := make(map[string]bool) // track lock state

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, msg *MessageEnvelope) error {
			lockState[string(msg.key)] = true
			return nil
		},
		ReleaseFunc: func(_ context.Context, msg *MessageEnvelope) error {
			lockState[string(msg.key)] = false
			return nil
		},
		IsLockedFunc: func(_ context.Context, msg *MessageEnvelope) bool {
			return lockState[string(msg.key)]
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-123"),
		headerData: &headerList{},
	}

	// Before redirect: key is not locked
	assert.False(t, tracker.IsInRetryChain(context.Background(), msg))

	// Redirect fails (producer error)
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))
	require.Error(t, err)

	// After failed redirect with successful rollback: key should NOT be locked
	assert.False(t, tracker.IsInRetryChain(context.Background(), msg),
		"Key should not be locked after successful rollback")
}

func TestErrorTracker_NotRetriableError_GoesDirectlyToDLQ(t *testing.T) {
	// NotRetriableError should bypass retry topic and go directly to DLQ
	// It should NOT acquire a lock
	var producedTopics []string

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopics = append(producedTopics, topic)
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			t.Error("Acquire should NOT be called for NotRetriableError")
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		WarnFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("invalid-order"),
		headerData: &headerList{},
	}

	// Redirect with NotRetriableError
	notRetriableErr := NewNotRetriableError(errors.New("invalid order format - permanent failure"))
	err = tracker.Redirect(context.Background(), msg, notRetriableErr)

	require.NoError(t, err)

	// Should go to DLQ, not retry topic
	require.Len(t, producedTopics, 1)
	assert.Equal(t, "dlq_orders", producedTopics[0], "NotRetriableError should go directly to DLQ")

	// Acquire should NOT have been called
	assert.Empty(t, mockCoordinator.AcquireCalls(), "Lock should not be acquired for NotRetriableError")
}

func TestErrorTracker_NotRetriableError_PreservesOriginalError(t *testing.T) {
	// Verify that NotRetriableError properly wraps and preserves the original error
	originalErr := errors.New("validation failed: order ID must be positive")
	wrappedErr := NewNotRetriableError(originalErr)

	// Error() should return original message
	assert.Equal(t, originalErr.Error(), wrappedErr.Error())

	// Unwrap() should return original error
	assert.Equal(t, originalErr, wrappedErr.Unwrap())

	// errors.Is should work through the wrapper
	require.ErrorIs(t, wrappedErr, originalErr)

	// errors.As should find NotRetriableError
	var notRetriable *NotRetriableError
	require.ErrorAs(t, wrappedErr, &notRetriable)
	assert.Equal(t, originalErr, notRetriable.Origin)
}

func TestErrorTracker_Close_WithTimeout(t *testing.T) {
	// Test that Close respects context timeout
	mockProducer := &ProducerMock{}
	mockCoordinator := &StateCoordinatorMock{
		CloseFunc: func(_ context.Context) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Close with sufficient timeout should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = tracker.Close(ctx)
	require.NoError(t, err)
}

func TestErrorTracker_Close_TimeoutReturnsError(t *testing.T) {
	// Test that Close returns error when timeout expires
	// Simulates a scenario where workers don't exit in time
	mockProducer := &ProducerMock{}

	// Coordinator that blocks forever on Close (simulating stuck worker)
	mockCoordinator := &StateCoordinatorMock{
		CloseFunc: func(ctx context.Context) error {
			<-ctx.Done() // Block until context canceled
			return ctx.Err()
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Close with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = tracker.Close(ctx)

	// Should return timeout error (either from workers or coordinator)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
		"Expected context timeout error, got: %v", err)
}

func TestErrorTracker_Redirect_HappyPath(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("fail"))
	require.NoError(t, err)

	assert.Len(t, mockCoordinator.AcquireCalls(), 1)
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	assert.Equal(t, "retry_orders", mockProducer.ProduceCalls()[0].Topic)
}

func TestErrorTracker_Redirect_AlreadyInRetry_SkipsLockAcquisition(t *testing.T) {
	// When a message is already in retry (has headerRetry="true"),
	// Redirect should NOT call Acquire again - the lock is already held
	var producedMsg Message

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, msg Message) error {
			producedMsg = msg
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			t.Error("Acquire should NOT be called for messages already in retry")
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message that's already in the retry chain
	headers := &headerList{}
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "existing-lock-id")
	_ = SetHeader[string](headers, headerTopic, "orders")
	_ = SetHeader[int](headers, HeaderRetryAttempt, 1)

	msg := &MessageEnvelope{
		topic:      "retry_orders", // Coming from retry topic
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	// Acquire should NOT have been called
	assert.Empty(t, mockCoordinator.AcquireCalls(), "Lock should not be re-acquired for retry messages")

	// Message should still be produced to retry topic
	assert.Len(t, mockProducer.ProduceCalls(), 1)

	// Verify the ID header is preserved
	idBytes, ok := producedMsg.Headers().Get(headerID)
	assert.True(t, ok)
	assert.Equal(t, "existing-lock-id", string(idBytes))
}

func TestErrorTracker_Redirect_IncrementsAttemptCounter(t *testing.T) {
	// Verify that each Redirect increments the attempt counter
	var producedMsg Message

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, msg Message) error {
			producedMsg = msg
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// First redirect (no existing attempt header)
	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("fail"))
	require.NoError(t, err)

	// Check attempt is 1
	attemptBytes, ok := producedMsg.Headers().Get(HeaderRetryAttempt)
	require.True(t, ok)
	assert.Equal(t, "1", string(attemptBytes))

	// Second redirect (simulating retry message with attempt=1)
	headers := &headerList{}
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "lock-id")
	_ = SetHeader[string](headers, headerTopic, "orders")
	_ = SetHeader[int](headers, HeaderRetryAttempt, 1)

	msg2 := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg2, errors.New("still failing"))
	require.NoError(t, err)

	// Check attempt is now 2
	attemptBytes, ok = producedMsg.Headers().Get(HeaderRetryAttempt)
	require.True(t, ok)
	assert.Equal(t, "2", string(attemptBytes))
}

func TestErrorTracker_Redirect_PreservesUserHeaders(t *testing.T) {
	// User-defined headers should be preserved across retries
	var producedMsg Message

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, msg Message) error {
			producedMsg = msg
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message with custom user headers
	headers := &headerList{}
	headers.Set("x-correlation-id", []byte("corr-123"))
	headers.Set("x-trace-id", []byte("trace-456"))
	headers.Set("x-custom-header", []byte("custom-value"))

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		payload:    []byte(`{"order": "data"}`),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("temporary failure"))
	require.NoError(t, err)

	// Verify user headers are preserved
	corrID, ok := producedMsg.Headers().Get("x-correlation-id")
	assert.True(t, ok, "x-correlation-id should be preserved")
	assert.Equal(t, "corr-123", string(corrID))

	traceID, ok := producedMsg.Headers().Get("x-trace-id")
	assert.True(t, ok, "x-trace-id should be preserved")
	assert.Equal(t, "trace-456", string(traceID))

	customHeader, ok := producedMsg.Headers().Get("x-custom-header")
	assert.True(t, ok, "x-custom-header should be preserved")
	assert.Equal(t, "custom-value", string(customHeader))

	// Retry headers should also be added
	_, ok = producedMsg.Headers().Get(HeaderRetryAttempt)
	assert.True(t, ok, "retry attempt header should be added")

	_, ok = producedMsg.Headers().Get(headerRetry)
	assert.True(t, ok, "retry flag header should be added")
}

func TestErrorTracker_Redirect_PreservesOriginalTopic(t *testing.T) {
	// When redirecting from retry topic, the original topic should be preserved
	var (
		producedMsg   Message
		producedTopic string
	)

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, msg Message) error {
			producedTopic = topic
			producedMsg = msg

			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message coming from retry topic with original topic in headers
	headers := &headerList{}
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "lock-id")
	_ = SetHeader[string](headers, headerTopic, "original-orders") // Original topic
	_ = SetHeader[int](headers, HeaderRetryAttempt, 1)

	msg := &MessageEnvelope{
		topic:      "retry_original-orders", // Current topic is retry
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	// Should produce to retry topic based on ORIGINAL topic
	assert.Equal(t, "retry_original-orders", producedTopic)

	// Original topic header should be preserved
	topicHeader, ok := producedMsg.Headers().Get(headerTopic)
	assert.True(t, ok)
	assert.Equal(t, "original-orders", string(topicHeader))
}

func TestErrorTracker_Redirect_AcquireFailure_ReturnsError(t *testing.T) {
	// If Acquire fails, Redirect should return error without producing
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			t.Error("Produce should NOT be called if Acquire fails")
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return errors.New("coordinator unavailable")
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire lock")
	assert.Contains(t, err.Error(), "coordinator unavailable")

	// Producer should not have been called
	assert.Empty(t, mockProducer.ProduceCalls())
}

func TestErrorTracker_Redirect_SetsRetryMetadataHeaders(t *testing.T) {
	// Verify all retry metadata headers are correctly set
	var producedMsg Message

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, msg Message) error {
			producedMsg = msg
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, msg *MessageEnvelope) error {
			// Simulate Acquire setting the ID header
			_ = SetHeader[string](msg.headerData, headerID, "generated-uuid-123")
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		payload:    []byte("payload"),
		headerData: &headerList{},
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))
	require.NoError(t, err)

	headers := producedMsg.Headers()

	// Verify all retry headers
	attemptBytes, ok := headers.Get(HeaderRetryAttempt)
	assert.True(t, ok, "HeaderRetryAttempt should be set")
	assert.Equal(t, "1", string(attemptBytes))

	maxBytes, ok := headers.Get(HeaderRetryMax)
	assert.True(t, ok, "HeaderRetryMax should be set")
	assert.Equal(t, "5", string(maxBytes))

	_, ok = headers.Get(HeaderRetryNextTime)
	assert.True(t, ok, "HeaderRetryNextTime should be set")

	_, ok = headers.Get(HeaderRetryOriginalTime)
	assert.True(t, ok, "HeaderRetryOriginalTime should be set")

	reasonBytes, ok := headers.Get(HeaderRetryReason)
	assert.True(t, ok, "HeaderRetryReason should be set")
	assert.Equal(t, "business error", string(reasonBytes))

	idBytes, ok := headers.Get(headerID)
	assert.True(t, ok, "headerID should be set")
	assert.Equal(t, "generated-uuid-123", string(idBytes))

	retryBytes, ok := headers.Get(headerRetry)
	assert.True(t, ok, "headerRetry should be set")
	assert.Equal(t, "true", string(retryBytes))

	topicBytes, ok := headers.Get(headerTopic)
	assert.True(t, ok, "headerTopic should be set")
	assert.Equal(t, "orders", string(topicBytes))
}

// --- Free() Tests ---

func TestErrorTracker_Free_HappyPath(t *testing.T) {
	// Free should call coordinator.Release
	var releasedMsg *MessageEnvelope

	mockCoordinator := &StateCoordinatorMock{
		ReleaseFunc: func(_ context.Context, msg *MessageEnvelope) error {
			releasedMsg = msg
			return nil
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	headers := &headerList{}
	_ = SetHeader[string](headers, headerID, "lock-id-123")
	_ = SetHeader[string](headers, headerTopic, "orders")

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Free(context.Background(), msg)
	require.NoError(t, err)

	assert.Len(t, mockCoordinator.ReleaseCalls(), 1)
	assert.Equal(t, []byte("order-1"), releasedMsg.key)
}

func TestErrorTracker_Free_CoordinatorFailure_ReturnsError(t *testing.T) {
	// If coordinator.Release fails, Free should return the error
	mockCoordinator := &StateCoordinatorMock{
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			return errors.New("coordinator release failed")
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.Free(context.Background(), msg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "coordinator release failed")
}

func TestErrorTracker_Free_WithMissingHeaders_StillCallsRelease(t *testing.T) {
	// Even without proper headers, Free should attempt Release
	// The coordinator is responsible for handling missing headers gracefully
	var releaseCalled bool

	mockCoordinator := &StateCoordinatorMock{
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			releaseCalled = true
			return nil
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message with no headers at all
	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.Free(context.Background(), msg)
	require.NoError(t, err)

	assert.True(t, releaseCalled, "Release should be called even with missing headers")
}

// --- SendToDLQ() Tests ---

func TestErrorTracker_SendToDLQ_HappyPath(t *testing.T) {
	var (
		producedTopic string
		producedMsg   Message
	)

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, msg Message) error {
			producedTopic = topic
			producedMsg = msg

			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 3

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		payload:    []byte(`{"order": "data"}`),
		headerData: &headerList{},
	}

	err = tracker.SendToDLQ(context.Background(), msg, errors.New("max retries exceeded"))
	require.NoError(t, err)

	// Should produce to DLQ topic
	assert.Equal(t, "dlq_orders", producedTopic)

	// Should preserve payload
	assert.JSONEq(t, `{"order": "data"}`, string(producedMsg.Value()))

	// Should have DLQ headers
	headers := producedMsg.Headers()

	reasonBytes, ok := headers.Get(HeaderDLQReason)
	assert.True(t, ok, "DLQ reason header should be set")
	assert.Equal(t, "max retries exceeded", string(reasonBytes))

	_, ok = headers.Get(HeaderDLQTimestamp)
	assert.True(t, ok, "DLQ timestamp header should be set")

	sourceBytes, ok := headers.Get(HeaderDLQSourceTopic)
	assert.True(t, ok, "DLQ source topic header should be set")
	assert.Equal(t, "orders", string(sourceBytes))
}

func TestErrorTracker_SendToDLQ_PreservesOriginalTopicFromHeader(t *testing.T) {
	var producedTopic string

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopic = topic
			return nil
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message from retry topic with original topic in header
	headers := &headerList{}
	_ = SetHeader[string](headers, headerTopic, "original-orders")

	msg := &MessageEnvelope{
		topic:      "retry_original-orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.SendToDLQ(context.Background(), msg, errors.New("permanent failure"))
	require.NoError(t, err)

	// Should use original topic for DLQ naming
	assert.Equal(t, "dlq_original-orders", producedTopic)
}

func TestErrorTracker_SendToDLQ_IncludesRetryAttemptCount(t *testing.T) {
	var producedMsg Message

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, msg Message) error {
			producedMsg = msg
			return nil
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message that has gone through 3 retry attempts
	headers := &headerList{}
	_ = SetHeader[int](headers, HeaderRetryAttempt, 3)
	_ = SetHeader[string](headers, headerTopic, "orders")

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.SendToDLQ(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	// DLQ should include retry attempts count
	attemptsBytes, ok := producedMsg.Headers().Get(HeaderDLQRetryAttempts)
	assert.True(t, ok, "DLQ retry attempts header should be set")
	assert.Equal(t, "3", string(attemptsBytes))
}

func TestErrorTracker_SendToDLQ_ProducerFailure_ReturnsError(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka unavailable")
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("order-1"),
		headerData: &headerList{},
	}

	err = tracker.SendToDLQ(context.Background(), msg, errors.New("some error"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka unavailable")
}

// --- handleMaxRetriesExceeded() Tests ---

func TestErrorTracker_MaxRetriesExceeded_SendsToDLQ(t *testing.T) {
	var producedTopic string

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopic = topic
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 3
	cfg.FreeOnDLQ = false

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
			WarnFunc:  func(_ string, _ ...interface{}) {},
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Message that has already reached max retries
	headers := &headerList{}
	_ = SetHeader[int](headers, HeaderRetryAttempt, 3) // Equal to MaxRetries
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "lock-id")
	_ = SetHeader[string](headers, headerTopic, "orders")

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	// Should go to DLQ, not retry topic
	assert.Equal(t, "dlq_orders", producedTopic)
}

func TestErrorTracker_MaxRetriesExceeded_FreeOnDLQ_True_ReleasesLock(t *testing.T) {
	var releaseCalled bool

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			releaseCalled = true
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 3
	cfg.FreeOnDLQ = true // Should release lock after DLQ

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
			WarnFunc:  func(_ string, _ ...interface{}) {},
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	headers := &headerList{}
	_ = SetHeader[int](headers, HeaderRetryAttempt, 3)
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "lock-id")
	_ = SetHeader[string](headers, headerTopic, "orders")

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	assert.True(t, releaseCalled, "Release should be called when FreeOnDLQ=true")
}

func TestErrorTracker_MaxRetriesExceeded_FreeOnDLQ_False_KeepsLock(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			t.Error("Release should NOT be called when FreeOnDLQ=false")
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 3
	cfg.FreeOnDLQ = false // Should NOT release lock after DLQ

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
			WarnFunc:  func(_ string, _ ...interface{}) {},
			ErrorFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	headers := &headerList{}
	_ = SetHeader[int](headers, HeaderRetryAttempt, 3)
	_ = SetHeader[string](headers, headerRetry, "true")
	_ = SetHeader[string](headers, headerID, "lock-id")
	_ = SetHeader[string](headers, headerTopic, "orders")

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("still failing"))
	require.NoError(t, err)

	// Release should NOT have been called
	assert.Empty(t, mockCoordinator.ReleaseCalls(), "Release should not be called when FreeOnDLQ=false")
}

// --- WaitForRetryTime() Tests ---

func TestErrorTracker_WaitForRetryTime_NoHeader_ReturnsImmediately(t *testing.T) {
	// If no retry time header exists, should return immediately
	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: &headerList{}, // No headers
	}

	start := time.Now()
	err = tracker.WaitForRetryTime(context.Background(), msg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "Should return immediately without header")
}

func TestErrorTracker_WaitForRetryTime_PastTime_ReturnsImmediately(t *testing.T) {
	// If retry time is in the past, should return immediately
	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Set retry time to 1 hour ago
	pastTime := time.Now().Add(-1 * time.Hour)
	headers := &headerList{}
	_ = SetHeader[time.Time](headers, HeaderRetryNextTime, pastTime)

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	start := time.Now()
	err = tracker.WaitForRetryTime(context.Background(), msg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "Should return immediately when time is in the past")
}

func TestErrorTracker_WaitForRetryTime_FutureTime_Waits(t *testing.T) {
	// If retry time is in the future, should wait
	// Note: Unix timestamps have second granularity, so we need to wait at least 1 second
	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Set retry time to 1.5 seconds in the future (to account for second granularity)
	futureTime := time.Now().Add(1500 * time.Millisecond)
	headers := &headerList{}
	_ = SetHeader[time.Time](headers, HeaderRetryNextTime, futureTime)

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	start := time.Now()
	err = tracker.WaitForRetryTime(context.Background(), msg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	// Should wait at least 500ms (second granularity means we might wait less than 1.5s)
	assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "Should wait for the scheduled time")
	assert.Less(t, elapsed, 2*time.Second, "Should not wait too long")
}

func TestErrorTracker_WaitForRetryTime_ContextCancellation_ReturnsError(t *testing.T) {
	// If context is canceled during wait, should return context error
	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Set retry time to 5 seconds in the future
	futureTime := time.Now().Add(5 * time.Second)
	headers := &headerList{}
	_ = SetHeader[time.Time](headers, HeaderRetryNextTime, futureTime)

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	// Cancel context after 50ms
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = tracker.WaitForRetryTime(ctx, msg)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Should return context error")
	assert.Less(t, elapsed, 200*time.Millisecond, "Should return quickly after context cancellation")
}

func TestErrorTracker_WaitForRetryTime_CorruptedTimestamp_ReturnsImmediately(t *testing.T) {
	// If timestamp header is corrupted, should log warning and return immediately
	var warningLogged bool

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			WarnFunc: func(msg string, _ ...interface{}) {
				if msg == "invalid retry timestamp header" {
					warningLogged = true
				}
			},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// Set corrupted timestamp header (not a valid Unix timestamp)
	headers := &headerList{}
	headers.Set(HeaderRetryNextTime, []byte("not-a-timestamp"))

	msg := &MessageEnvelope{
		topic:      "retry_orders",
		key:        []byte("order-1"),
		headerData: headers,
	}

	start := time.Now()
	err = tracker.WaitForRetryTime(context.Background(), msg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "Should return immediately with corrupted header")
	assert.True(t, warningLogged, "Should log warning about invalid timestamp")
}

// --- ensureTopicsExist() Tests ---

func TestErrorTracker_ensureTopicsExist_AutoCreationDisabled_MissingTopics_ReturnsError(t *testing.T) {
	// When DisableAutoTopicCreation=true and topics don't exist, should return error
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{}, nil // No topics found
		},
	}

	cfg := newTestConfig()
	cfg.DisableAutoTopicCreation = true

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	err = tracker.ensureTopicsExist(context.Background(), "orders")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "required topics missing")
	assert.Contains(t, err.Error(), "auto-creation disabled")
}

func TestErrorTracker_ensureTopicsExist_AutoCreationDisabled_TopicsExist_NoError(t *testing.T) {
	// When DisableAutoTopicCreation=true and topics exist, should succeed
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, topics []string) ([]TopicMetadata, error) {
			result := make([]TopicMetadata, 0, len(topics))
			for _, t := range topics {
				result = append(result, &topicMetadataMock{name: t, partitions: 3})
			}

			return result, nil
		},
	}

	cfg := newTestConfig()
	cfg.DisableAutoTopicCreation = true

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	err = tracker.ensureTopicsExist(context.Background(), "orders")

	require.NoError(t, err)
}

func TestErrorTracker_ensureTopicsExist_AutoCreation_CreatesTopics(t *testing.T) {
	// When auto-creation enabled, should create retry and DLQ topics
	var (
		createdTopics []string
		mu            sync.Mutex
	)

	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, topics []string) ([]TopicMetadata, error) {
			// Return metadata for primary topic only
			if len(topics) == 1 && topics[0] == "orders" {
				return []TopicMetadata{&topicMetadataMock{name: "orders", partitions: 6}}, nil
			}

			return []TopicMetadata{}, nil
		},
		CreateTopicFunc: func(_ context.Context, topic string, _ int32, _ int16, _ map[string]string) error {
			mu.Lock()

			createdTopics = append(createdTopics, topic)

			mu.Unlock()

			return nil
		},
	}

	cfg := newTestConfig()
	cfg.DisableAutoTopicCreation = false

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	err = tracker.ensureTopicsExist(context.Background(), "orders")

	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, createdTopics, 2)
	assert.Contains(t, createdTopics, "retry_orders")
	assert.Contains(t, createdTopics, "dlq_orders")
}

func TestErrorTracker_ensureTopicsExist_UsesConfiguredPartitions(t *testing.T) {
	// When RetryTopicPartitions is set in config, should use that instead of primary topic
	var retryPartitions, dlqPartitions int32

	mockAdmin := &AdminMock{
		CreateTopicFunc: func(_ context.Context, topic string, partitions int32, _ int16, _ map[string]string) error {
			switch topic {
			case "retry_orders":
				retryPartitions = partitions
			case "dlq_orders":
				dlqPartitions = partitions
			}

			return nil
		},
	}

	cfg := newTestConfig()
	cfg.RetryTopicPartitions = 12 // Explicitly set

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	err = tracker.ensureTopicsExist(context.Background(), "orders")

	require.NoError(t, err)
	assert.Equal(t, int32(12), retryPartitions)
	assert.Equal(t, int32(12), dlqPartitions)
}

// topicMetadataMock implements TopicMetadata for tests
type topicMetadataMock struct {
	name       string
	partitions int32
}

func (m *topicMetadataMock) Name() string                      { return m.name }
func (m *topicMetadataMock) Partitions() int32                 { return m.partitions }
func (m *topicMetadataMock) PartitionOffsets() map[int32]int64 { return nil }

// --- NewResilientHandler() Tests ---

func TestErrorTracker_NewResilientHandler_KeyInRetryChain_AutoRedirects(t *testing.T) {
	// When a key is already in the retry chain, new messages should be auto-redirected
	var (
		userHandlerCalled bool
		producedTopic     string
	)

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopic = topic
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		IsLockedFunc: func(_ context.Context, _ *MessageEnvelope) bool {
			return true // Key is locked
		},
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	// User handler that should NOT be called
	userHandler := ConsumerHandlerFunc(func(_ context.Context, _ Message) error {
		userHandlerCalled = true
		return nil
	})

	resilientHandler := tracker.NewResilientHandler(userHandler)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("locked-key"),
		headerData: &headerList{},
	}

	err = resilientHandler.Handle(context.Background(), msg)
	require.NoError(t, err)

	// User handler should NOT have been called (key is locked)
	assert.False(t, userHandlerCalled, "User handler should not be called when key is in retry chain")

	// Message should be redirected to retry topic
	assert.Equal(t, "retry_orders", producedTopic)
}

func TestErrorTracker_NewResilientHandler_Success_NoLockManagement(t *testing.T) {
	// On success, main topic messages don't need lock management
	var userHandlerCalled bool

	mockCoordinator := &StateCoordinatorMock{
		IsLockedFunc: func(_ context.Context, _ *MessageEnvelope) bool {
			return false // Key is NOT locked
		},
		ReleaseFunc: func(_ context.Context, _ *MessageEnvelope) error {
			t.Error("Release should NOT be called for main topic messages")
			return nil
		},
	}

	cfg := newTestConfig()
	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	userHandler := ConsumerHandlerFunc(func(_ context.Context, _ Message) error {
		userHandlerCalled = true
		return nil // Success
	})

	resilientHandler := tracker.NewResilientHandler(userHandler)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("new-key"),
		headerData: &headerList{},
	}

	err = resilientHandler.Handle(context.Background(), msg)
	require.NoError(t, err)

	assert.True(t, userHandlerCalled)
	// Release should NOT have been called
	assert.Empty(t, mockCoordinator.ReleaseCalls())
}

func TestErrorTracker_NewResilientHandler_Failure_StartsRetryChain(t *testing.T) {
	// On failure, message should be redirected to retry topic
	var producedTopic string

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopic = topic
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		IsLockedFunc: func(_ context.Context, _ *MessageEnvelope) bool {
			return false // Key is NOT locked
		},
		AcquireFunc: func(_ context.Context, _ string, _ *MessageEnvelope) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	userHandler := ConsumerHandlerFunc(func(_ context.Context, _ Message) error {
		return errors.New("processing failed") // Failure
	})

	resilientHandler := tracker.NewResilientHandler(userHandler)

	msg := &MessageEnvelope{
		topic:      "orders",
		key:        []byte("failing-key"),
		headerData: &headerList{},
	}

	err = resilientHandler.Handle(context.Background(), msg)
	require.NoError(t, err) // Handler itself succeeds (redirects the message)

	// Lock should have been acquired
	assert.Len(t, mockCoordinator.AcquireCalls(), 1)

	// Message should be redirected to retry topic
	assert.Equal(t, "retry_orders", producedTopic)
}

// --- NewErrorTracker Validation Tests ---

func TestNewErrorTracker_NilConfig_ReturnsError(t *testing.T) {
	_, err := NewErrorTracker(
		nil, // nil config
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config cannot be nil")
}

func TestNewErrorTracker_NilLogger_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		nil, // nil logger
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestNewErrorTracker_NilProducer_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		nil, // nil producer
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "producer cannot be nil")
}

func TestNewErrorTracker_NilConsumerFactory_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		nil, // nil consumerFactory
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumerFactory cannot be nil")
}

func TestNewErrorTracker_NilAdmin_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		nil, // nil admin
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin cannot be nil")
}

func TestNewErrorTracker_NilCoordinator_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil, // nil coordinator
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "coordinator cannot be nil")
}

func TestNewErrorTracker_NilBackoff_ReturnsError(t *testing.T) {
	cfg := newTestConfig()

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		nil, // nil backoff
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "backoff strategy cannot be nil")
}

func TestNewErrorTracker_InvalidConfig_ReturnsError(t *testing.T) {
	cfg := NewDefaultConfig()
	// Missing GroupID makes config invalid

	_, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GroupID")
}

// --- Topic Name Generation Tests ---

func TestErrorTracker_TopicNames(t *testing.T) {
	cfg := newTestConfig()
	cfg.RetryTopicPrefix = "retry"
	cfg.RedirectTopicPrefix = "redirect"
	cfg.DLQTopicPrefix = "dlq"

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		&StateCoordinatorMock{},
		&mockBackoff{},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, "retry_orders", tracker.RetryTopic("orders"))
	assert.Equal(t, "redirect_orders", tracker.RedirectTopic("orders"))
	assert.Equal(t, "dlq_orders", tracker.DLQTopic("orders"))

	// Test with complex topic name
	assert.Equal(t, "retry_user-events-v2", tracker.RetryTopic("user-events-v2"))
	assert.Equal(t, "redirect_user-events-v2", tracker.RedirectTopic("user-events-v2"))
	assert.Equal(t, "dlq_user-events-v2", tracker.DLQTopic("user-events-v2"))
}
