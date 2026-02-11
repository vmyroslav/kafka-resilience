package resilience

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	// HeaderRetryAttempt current retry count
	HeaderRetryAttempt = "x-retry-attempt"
	// HeaderRetryMax maximum retries allowed
	HeaderRetryMax = "x-retry-max"
	// HeaderRetryNextTime next retry timestamp (Unix)
	HeaderRetryNextTime = "x-retry-next-time"
	// HeaderRetryOriginalTime first failure timestamp (Unix)
	HeaderRetryOriginalTime = "x-retry-original-time"
	// HeaderRetryReason error message from last failure
	HeaderRetryReason = "x-retry-reason"
	// HeaderDLQReason error reason for sending to DLQ
	HeaderDLQReason = "x-dlq-reason"
	// HeaderDLQTimestamp timestamp when message was sent to DLQ
	HeaderDLQTimestamp = "x-dlq-timestamp"
	// HeaderDLQSourceTopic original topic before DLQ
	HeaderDLQSourceTopic = "x-dlq-source-topic"
	// HeaderDLQRetryAttempts number of retries attempted
	HeaderDLQRetryAttempts = "x-dlq-retry-attempts"
	// HeaderDLQOriginalFailureTime time of the first failure
	HeaderDLQOriginalFailureTime = "x-dlq-original-failure-time"
)

// Internal header constants used by the coordinator and retry logic.
const (
	headerCoordinatorID = "x-coordinator-id"
	headerTopic         = "topic"
	headerID            = "id"
	headerRetry         = "retry"
	headerKey           = "key"
)

// Message represents a Kafka message
type Message interface {
	Topic() string
	Partition() int32
	Offset() int64
	Key() []byte
	Value() []byte
	Headers() Headers
	Timestamp() time.Time
}

// Headers represents message headers.
type Headers interface {
	// Get retrieves a header value by key
	Get(key string) ([]byte, bool)

	// Set adds or updates a header
	Set(key string, value []byte)

	// Delete removes a header
	Delete(key string)

	// All returns all headers as a map
	All() map[string][]byte

	// Clone creates a deep copy of headers
	Clone() Headers

	// Range calls fn sequentially for each header. If fn returns false, range stops the iteration.
	Range(fn func(key string, value []byte) bool)
}

// MessageEnvelope is the internal message representation.
type MessageEnvelope struct {
	timestamp  time.Time
	topic      string
	key        []byte
	payload    []byte
	headerData *headerList
	partition  int32
	offset     int64
}

// NewHeaders creates a new empty Headers instance.
func NewHeaders() Headers {
	return &headerList{}
}

// NewMessageEnvelope creates a new MessageEnvelope with the given topic, key, value, and headers.
func NewMessageEnvelope(topic string, key, value []byte, headers Headers) *MessageEnvelope {
	hl, _ := headers.(*headerList)
	if hl == nil {
		hl = &headerList{}

		if headers != nil {
			headers.Range(func(key string, value []byte) bool {
				hl.Set(key, value)
				return true
			})
		}
	}

	return &MessageEnvelope{
		topic:      topic,
		key:        key,
		payload:    value,
		headerData: hl,
		timestamp:  time.Now(),
	}
}

// NewFromMessage creates an MessageEnvelope by copying all fields and headers from the given Message.
func NewFromMessage(msg Message) *MessageEnvelope {
	im := &MessageEnvelope{
		topic:      msg.Topic(),
		partition:  msg.Partition(),
		offset:     msg.Offset(),
		key:        msg.Key(),
		payload:    msg.Value(),
		timestamp:  msg.Timestamp(),
		headerData: &headerList{},
	}

	msg.Headers().Range(func(key string, value []byte) bool {
		im.headerData.Set(key, value)
		return true
	})

	return im
}

// Topic returns the topic name of the message.
func (m *MessageEnvelope) Topic() string {
	return m.topic
}

// SetTopic sets the topic name of the message.
func (m *MessageEnvelope) SetTopic(topic string) {
	m.topic = topic
}

// Partition returns the partition of the message.
func (m *MessageEnvelope) Partition() int32 {
	return m.partition
}

// SetPartition sets the partition of the message.
func (m *MessageEnvelope) SetPartition(partition int32) {
	m.partition = partition
}

// Offset returns the offset of the message.
func (m *MessageEnvelope) Offset() int64 {
	return m.offset
}

// SetOffset sets the offset of the message.
func (m *MessageEnvelope) SetOffset(offset int64) {
	m.offset = offset
}

// Value returns the message payload.
func (m *MessageEnvelope) Value() []byte {
	return m.payload
}

// Key returns the message key.
func (m *MessageEnvelope) Key() []byte {
	return m.key
}

// Timestamp returns the message timestamp.
func (m *MessageEnvelope) Timestamp() time.Time {
	return m.timestamp
}

// Headers returns the message headers, initializing the headerData if nil.
func (m *MessageEnvelope) Headers() Headers {
	if m.headerData == nil {
		m.headerData = &headerList{}
	}

	return m.headerData
}

type header struct {
	Key   []byte
	Value []byte
}

type headerList struct {
	list []header
	mu   sync.RWMutex
}

func (h *headerList) Get(key string) ([]byte, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, hdr := range h.list {
		if string(hdr.Key) == key {
			return hdr.Value, true
		}
	}

	return nil, false
}

func (h *headerList) Set(key string, value []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// find and update existing header in list to preserve order
	for i, hdr := range h.list {
		if string(hdr.Key) == key {
			h.list[i].Value = value
			return
		}
	}

	// add new header if not found
	h.list = append(h.list, header{
		Key:   []byte(key),
		Value: value,
	})
}

func (h *headerList) All() map[string][]byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string][]byte, len(h.list))
	for _, hdr := range h.list {
		result[string(hdr.Key)] = hdr.Value
	}

	return result
}

func (h *headerList) Delete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newHeaders := make([]header, 0, len(h.list))
	for _, hdr := range h.list {
		if string(hdr.Key) != key {
			newHeaders = append(newHeaders, hdr)
		}
	}

	h.list = newHeaders
}

func (h *headerList) Clone() Headers {
	h.mu.RLock()
	defer h.mu.RUnlock()

	cloned := &headerList{
		list: make([]header, len(h.list)),
	}

	for i, hdr := range h.list {
		cloned.list[i] = header{
			Key:   append([]byte(nil), hdr.Key...),
			Value: append([]byte(nil), hdr.Value...),
		}
	}

	return cloned
}

func (h *headerList) Range(fn func(key string, value []byte) bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, hdr := range h.list {
		if !fn(string(hdr.Key), hdr.Value) {
			break
		}
	}
}

func GetHeaderValue[T any](h Headers, key string) (T, bool) {
	var zero T
	if h == nil {
		return zero, false
	}

	var (
		val   []byte
		found bool
	)

	val, found = h.Get(key)
	if !found {
		return zero, false
	}

	valStr := string(val)

	var anyVal any = zero
	switch anyVal.(type) {
	case string:
		v, ok := any(valStr).(T)

		return v, ok
	case int:
		i, err := strconv.Atoi(valStr)
		if err != nil {
			return zero, false
		}

		v, ok := any(i).(T)

		return v, ok
	case time.Time:
		unix, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return zero, false
		}

		v, ok := any(time.Unix(unix, 0)).(T)

		return v, ok
	default:
		return zero, false
	}
}

// SetHeader updates or adds a header with the given key and value.
// Supported types: string, int, time.Time. Returns an error if an unsupported type is provided.
// If h is nil, this is a no-op and returns nil.
func SetHeader[T any](h Headers, key string, value T) error {
	if h == nil {
		return nil
	}

	var valStr string

	// convert T to string based on its type
	switch v := any(value).(type) {
	case string:
		valStr = v
	case int:
		valStr = strconv.Itoa(v)
	case time.Time:
		valStr = strconv.FormatInt(v.Unix(), 10)
	default:
		return fmt.Errorf(
			"SetHeader: unsupported type %T (supported: string, int, time.Time)",
			value,
		)
	}

	h.Set(key, []byte(valStr))

	return nil
}
