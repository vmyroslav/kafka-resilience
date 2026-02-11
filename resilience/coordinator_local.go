package resilience

import (
	"context"
	"sync"
)

// localStateCoordinator implements StateCoordinator for local in-memory locking.
// It manages reference counting for keys to ensure strict ordering within a single process.
type localStateCoordinator struct {
	lm lockMap
	mu sync.RWMutex
}

// newLocalStateCoordinator creates a new localStateCoordinator.
func newLocalStateCoordinator() *localStateCoordinator {
	return &localStateCoordinator{
		lm: make(lockMap),
	}
}

// Start is a no-op for localStateCoordinator as it requires no background processes.
func (l *localStateCoordinator) Start(_ context.Context, _ string) error {
	return nil
}

// Acquire locks the key in local memory.
func (l *localStateCoordinator) Acquire(_ context.Context, originalTopic string, msg *MessageEnvelope) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	key := string(msg.key)
	l.lm.incrementRef(originalTopic, key)

	return nil
}

// Release unlocks the key in local memory.
func (l *localStateCoordinator) Release(_ context.Context, msg *MessageEnvelope) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	topic, ok := GetHeaderValue[string](msg.headerData, headerTopic)
	if !ok {
		topic = msg.topic
	}

	key := string(msg.key)

	newCount, exists := l.lm.decrementRef(topic, key)
	if !exists {
		return nil
	}

	if newCount <= 0 {
		l.lm.removeKey(topic, key)
	}

	if l.lm.isTopicEmpty(topic) {
		l.lm.removeTopic(topic)
	}

	return nil
}

// IsLocked checks if the key is currently locked in local memory.
func (l *localStateCoordinator) IsLocked(_ context.Context, msg *MessageEnvelope) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	count, exists := l.lm.getRefCount(msg.topic, string(msg.key))

	return exists && count > 0
}

// Synchronize is a no-op for localStateCoordinator.
func (l *localStateCoordinator) Synchronize(_ context.Context) error {
	return nil
}

// Close is a no-op for localStateCoordinator (no background workers to stop).
func (l *localStateCoordinator) Close(_ context.Context) error {
	return nil
}

// Errors returns nil because localStateCoordinator has no background goroutines.
func (l *localStateCoordinator) Errors() <-chan error {
	return nil
}

// topic map ['topic-name' => ['topic-key' => refCount]].
// Reference counting ensures keys are unlocked only when all messages with that key complete.
type lockMap map[string]map[string]int

// getRefCount returns the reference count for a topic/key pair.
// Returns (count, exists) - exists is false if topic or key doesn't exist.
func (lm lockMap) getRefCount(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	count, ok := lm[topic][key]

	return count, ok
}

// incrementRef increments the reference count for a topic/key pair
func (lm lockMap) incrementRef(topic, key string) int {
	if lm[topic] == nil {
		lm[topic] = make(map[string]int)
	}

	lm[topic][key]++

	return lm[topic][key]
}

// decrementRef decrements the reference count for a topic/key pair.
// Returns the new reference count and whether the key existed.
func (lm lockMap) decrementRef(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	if _, ok := lm[topic][key]; !ok {
		return 0, false
	}

	lm[topic][key]--

	return lm[topic][key], true
}

// removeKey removes a key from a topic
func (lm lockMap) removeKey(topic, key string) {
	if lm[topic] == nil {
		return
	}

	delete(lm[topic], key)
}

// removeTopic removes an entire topic and all its keys.
func (lm lockMap) removeTopic(topic string) {
	delete(lm, topic)
}

// isTopicEmpty returns true if topic has no keys or doesn't exist.
func (lm lockMap) isTopicEmpty(topic string) bool {
	return len(lm[topic]) == 0
}
