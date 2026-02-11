package resilience

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testGroupID    = "test-group"
	testSamePrefix = "same"
)

func newValidTestConfig() *Config {
	cfg := NewDefaultConfig()
	cfg.GroupID = testGroupID

	return cfg
}

func TestConfig_Validate_Success(t *testing.T) {
	t.Parallel()

	cfg := newValidTestConfig()

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestConfig_Validate_MissingGroupID(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GroupID is required")
}

func TestConfig_Validate_EmptyPrefixes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		modifyConfig  func(*Config)
		expectedError string
	}{
		{
			name: "empty RetryTopicPrefix",
			modifyConfig: func(c *Config) {
				c.RetryTopicPrefix = ""
			},
			expectedError: "RetryTopicPrefix cannot be empty",
		},
		{
			name: "empty RedirectTopicPrefix",
			modifyConfig: func(c *Config) {
				c.RedirectTopicPrefix = ""
			},
			expectedError: "RedirectTopicPrefix cannot be empty",
		},
		{
			name: "empty DLQTopicPrefix",
			modifyConfig: func(c *Config) {
				c.DLQTopicPrefix = ""
			},
			expectedError: "DLQTopicPrefix cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewDefaultConfig()
			cfg.GroupID = testGroupID
			tc.modifyConfig(cfg)

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestConfig_Validate_InvalidNumericValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		modifyConfig  func(*Config)
		expectedError string
	}{
		{
			name: "negative MaxRetries",
			modifyConfig: func(c *Config) {
				c.MaxRetries = -1
			},
			expectedError: "MaxRetries must be >= 0",
		},
		{
			name: "zero ReplicationFactor",
			modifyConfig: func(c *Config) {
				c.ReplicationFactor = 0
			},
			expectedError: "ReplicationFactor must be >= 1",
		},
		{
			name: "negative ReplicationFactor",
			modifyConfig: func(c *Config) {
				c.ReplicationFactor = -1
			},
			expectedError: "ReplicationFactor must be >= 1",
		},
		{
			name: "negative RetryTopicPartitions",
			modifyConfig: func(c *Config) {
				c.RetryTopicPartitions = -1
			},
			expectedError: "RetryTopicPartitions must be >= 0",
		},
		{
			name: "negative StateRestoreTimeout",
			modifyConfig: func(c *Config) {
				c.StateRestoreTimeout = -1
			},
			expectedError: "StateRestoreTimeout must be >= 0",
		},
		{
			name: "negative StateRestoreIdleTimeout",
			modifyConfig: func(c *Config) {
				c.StateRestoreIdleTimeout = -1
			},
			expectedError: "StateRestoreIdleTimeout must be >= 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewDefaultConfig()
			cfg.GroupID = testGroupID
			tc.modifyConfig(cfg)

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestConfig_Validate_PrefixCollisions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		modifyConfig  func(*Config)
		expectedError string
	}{
		{
			name: "RetryTopicPrefix equals RedirectTopicPrefix",
			modifyConfig: func(c *Config) {
				c.RetryTopicPrefix = testSamePrefix
				c.RedirectTopicPrefix = testSamePrefix
			},
			expectedError: "RetryTopicPrefix and RedirectTopicPrefix must be different",
		},
		{
			name: "RetryTopicPrefix equals DLQTopicPrefix",
			modifyConfig: func(c *Config) {
				c.RetryTopicPrefix = testSamePrefix
				c.DLQTopicPrefix = testSamePrefix
			},
			expectedError: "RetryTopicPrefix and DLQTopicPrefix must be different",
		},
		{
			name: "RedirectTopicPrefix equals DLQTopicPrefix",
			modifyConfig: func(c *Config) {
				c.RedirectTopicPrefix = testSamePrefix
				c.DLQTopicPrefix = testSamePrefix
			},
			expectedError: "RedirectTopicPrefix and DLQTopicPrefix must be different",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewDefaultConfig()
			cfg.GroupID = testGroupID
			tc.modifyConfig(cfg)

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestConfig_Validate_MultipleErrors(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		// GroupID is empty
		RetryTopicPrefix:    "",
		RedirectTopicPrefix: "",
		DLQTopicPrefix:      "",
		MaxRetries:          -1,
		ReplicationFactor:   0,
	}

	err := cfg.Validate()
	require.Error(t, err)

	// should contain multiple errors
	assert.Contains(t, err.Error(), "GroupID is required")
	assert.Contains(t, err.Error(), "RetryTopicPrefix cannot be empty")
	assert.Contains(t, err.Error(), "MaxRetries must be >= 0")
	assert.Contains(t, err.Error(), "ReplicationFactor must be >= 1")
}

func TestNewDefaultConfig_ReplicationFactor(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()

	// verify default replication factor
	assert.Equal(t, int16(1), cfg.ReplicationFactor)
}

func TestConfig_Validate_ZeroMaxRetries(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()
	cfg.GroupID = testGroupID
	cfg.MaxRetries = 0 // valid - send directly to DLQ on first failure

	err := cfg.Validate()
	require.NoError(t, err)
}
