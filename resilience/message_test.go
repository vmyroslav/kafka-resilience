package resilience

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderList(t *testing.T) {
	t.Parallel()

	type args struct {
		key string
		val string
	}

	tests := []args{
		{"key", "value"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()

			var hl headerList

			err := SetHeader[string](&hl, tt.key, tt.val)
			require.NoError(t, err)

			value, ok := GetHeaderValue[string](&hl, tt.key)
			if !ok || value != tt.val {
				t.Errorf("GetHeaderValue() got = %v, want %v", value, tt.val)
			}
		})
	}
}

func TestHeaderListUpdate(t *testing.T) {
	t.Parallel()

	var hl headerList

	err := SetHeader[string](&hl, "key", "value-1")
	require.NoError(t, err)
	err = SetHeader[string](&hl, "key", "value-2")
	require.NoError(t, err)

	value, ok := GetHeaderValue[string](&hl, "key")

	assert.True(t, ok, "value should be found")
	assert.Equal(t, "value-2", value, "expected updated value")
	assert.Len(t, hl.list, 1, "should have only one header, not duplicates")
}

func TestHeaderGet(t *testing.T) {
	t.Parallel()

	var hl headerList

	err := SetHeader[string](&hl, "key1", "value-1")
	require.NoError(t, err)
	err = SetHeader[int](&hl, "key2", 42)
	require.NoError(t, err)
	err = SetHeader[time.Time](&hl, "key3", time.Unix(1234567890, 0))
	require.NoError(t, err)

	val1, ok := GetHeaderValue[string](&hl, "key1")
	assert.True(t, ok, "string value should be found")
	assert.Equal(t, "value-1", val1)

	val2, ok := GetHeaderValue[int](&hl, "key2")
	assert.True(t, ok, "int value should be found")
	assert.Equal(t, 42, val2)

	val3, ok := GetHeaderValue[time.Time](&hl, "key3")
	assert.True(t, ok, "time value should be found")
	assert.Equal(t, time.Unix(1234567890, 0), val3)
}

func TestSetHeader_UnsupportedType(t *testing.T) {
	t.Parallel()

	var hl headerList

	// unsupported type (float64)
	err := SetHeader[float64](&hl, "key", 3.14)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
	assert.Contains(t, err.Error(), "float64")

	// unsupported type (bool)
	err = SetHeader[bool](&hl, "key", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
	assert.Contains(t, err.Error(), "bool")

	assert.Empty(t, hl.list)
}
