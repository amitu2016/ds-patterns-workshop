package listwatch

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"kubelite/pkg/retry"
	"kubelite/pkg/storage"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// zapLogger adapts zap.Logger to our Logger interface
type zapLogger struct {
	log *zap.Logger
}

func (z *zapLogger) Info(msg string, keysAndValues ...interface{}) {
	z.log.Sugar().Infow(msg, keysAndValues...)
}

func (z *zapLogger) Error(msg string, keysAndValues ...interface{}) {
	z.log.Sugar().Errorw(msg, keysAndValues...)
}

// setupLogger creates a new zap logger for testing
func setupLogger(t *testing.T) Logger {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := config.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = logger.Sync() })
	return &zapLogger{log: logger}
}

func TestNewListWatch(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		endpoints   []string
		opts        Options
		expectError bool
	}{
		{
			name:        "valid configuration",
			prefix:      "/test/prefix",
			endpoints:   []string{"localhost:2379"},
			opts:        DefaultOptions(),
			expectError: false,
		},
		{
			name:        "empty prefix",
			prefix:      "",
			endpoints:   []string{"localhost:2379"},
			opts:        DefaultOptions(),
			expectError: true,
		},
		{
			name:      "custom options",
			prefix:    "/test/prefix",
			endpoints: []string{"localhost:2379"},
			opts: Options{
				DialTimeout: 1 * time.Second,
				RetryOpts: retry.Options{
					InitialDelay: 100 * time.Millisecond,
					MaxDelay:     1 * time.Second,
					Multiplier:   1.5,
				},
				EventChannelBuffer: 50,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := setupLogger(t)
			lw, err := NewListWatch(tt.endpoints, tt.prefix, tt.opts, logger)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, lw)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, lw)
				assert.Equal(t, tt.prefix, lw.watchPrefix)
				assert.Equal(t, tt.opts, lw.opts)
				assert.NotNil(t, lw.metrics)
				assert.NotNil(t, lw.logger)
			}
		})
	}
}

// setupEtcd starts an embedded etcd server and returns its cleanup function
func setupEtcd(t *testing.T) (*embed.Etcd, string, func()) {
	// Start embedded etcd
	etcdServer, port, err := storage.StartEmbeddedEtcd()
	require.NoError(t, err)

	// Create cleanup function that only stops once
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			storage.StopEmbeddedEtcd(etcdServer)
		})
	}

	// Return etcd server and endpoint
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	return etcdServer, endpoint, cleanup
}

// testEventCondition defines a condition to wait for in tests
type testEventCondition struct {
	description string
	condition   func(Event) bool
}

// waitForEvents waits for a condition to be met on the event channel
func waitForEvents(t *testing.T, ch <-chan Event, timeout time.Duration, cond testEventCondition) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %v waiting for %s", time.Since(start), cond.description)
		case event, ok := <-ch:
			if !ok {
				return fmt.Errorf("channel closed after %v while waiting for %s", time.Since(start), cond.description)
			}
			t.Logf("[%v] Received event while waiting for %s: type=%s key=%s value=%s",
				time.Since(start), cond.description, event.Type, event.Key, string(event.Value))
			if cond.condition(event) {
				return nil
			}
		}
	}
}

func TestListWatch_RetryBehavior(t *testing.T) {
	// Setup embedded etcd
	_, endpoint, cleanup := setupEtcd(t)
	defer cleanup()

	// Create ListWatch with short retry intervals for testing
	opts := Options{
		DialTimeout: 1 * time.Second,
		RetryOpts: retry.Options{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     50 * time.Millisecond,
			Multiplier:   1.5,
		},
		EventChannelBuffer: 10,
	}
	logger := setupLogger(t)
	lw, err := NewListWatch([]string{endpoint}, "/test/retry/", opts, logger)
	require.NoError(t, err)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start ListAndWatch
	ch, stopWatch, err := lw.ListAndWatch(ctx)
	require.NoError(t, err)
	defer stopWatch()

	// Wait a bit for watch to be established
	time.Sleep(100 * time.Millisecond)

	// Add a key and wait for the event
	_, err = lw.etcdCli.Put(ctx, "/test/retry/key1", "value1")
	require.NoError(t, err)

	// Wait for the initial Added event
	err = waitForEvents(t, ch, 3*time.Second, testEventCondition{
		description: "initial Added event",
		condition: func(event Event) bool {
			return event.Type == Added && event.Key == "/test/retry/key1"
		},
	})
	require.NoError(t, err)

	// Create a channel to collect error events
	errorEvents := make([]Event, 0)

	// Force a watch failure by stopping etcd
	cleanup()

	// Wait for error events with timeout
	timeoutCh := time.After(10 * time.Second)
	expectedErrors := 1

	// Track when we started waiting
	start := time.Now()

	for len(errorEvents) < expectedErrors {
		select {
		case event, ok := <-ch:
			if !ok {
				elapsed := time.Since(start)
				t.Logf("Watch channel closed after %v. Error events received: %d", elapsed, len(errorEvents))
				for i, e := range errorEvents {
					t.Logf("Error %d: %s", i+1, string(e.Value))
				}
				require.GreaterOrEqual(t, len(errorEvents), expectedErrors,
					"Should receive at least one error event before channel close")
				return
			}

			t.Logf("Received event after %v: type=%s value=%s", time.Since(start), event.Type, string(event.Value))
			if event.Type == Error {
				errorEvents = append(errorEvents, event)
				t.Logf("Error event count: %d", len(errorEvents))
			}

		case <-timeoutCh:
			elapsed := time.Since(start)
			t.Logf("Timeout after %v. Received %d error events", elapsed, len(errorEvents))
			for i, e := range errorEvents {
				t.Logf("Error %d: %s", i+1, string(e.Value))
			}
			require.GreaterOrEqual(t, len(errorEvents), expectedErrors,
				"Should receive at least one error event before timeout")
			return
		}
	}

	// Log final summary
	t.Logf("Test completed successfully. Error events received: %d", len(errorEvents))
	for i, e := range errorEvents {
		t.Logf("Error %d: %s", i+1, string(e.Value))
	}
}

func TestListWatch_Integration(t *testing.T) {
	// Setup embedded etcd
	_, endpoint, cleanup := setupEtcd(t)
	defer cleanup()

	// Create ListWatch with test options
	opts := Options{
		DialTimeout: 1 * time.Second,
		RetryOpts: retry.Options{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.5,
		},
		EventChannelBuffer: 50,
	}
	logger := setupLogger(t)
	lw, err := NewListWatch([]string{endpoint}, "/test/prefix/", opts, logger)
	require.NoError(t, err)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start ListAndWatch
	ch, stopWatch, err := lw.ListAndWatch(ctx)
	require.NoError(t, err)
	defer stopWatch()

	// Give ListAndWatch a moment to establish the watch
	time.Sleep(100 * time.Millisecond)

	// Create test key-value pairs
	_, err = lw.etcdCli.Put(ctx, "/test/prefix/key1", "value1")
	require.NoError(t, err)

	// Wait for first event
	timeout := time.After(5 * time.Second)
	select {
	case event := <-ch:
		assert.Equal(t, Added, event.Type)
		assert.Equal(t, "/test/prefix/key1", event.Key)
		assert.Equal(t, "value1", string(event.Value))
	case <-timeout:
		t.Fatal("timeout waiting for first event")
	}

	// Add second key
	_, err = lw.etcdCli.Put(ctx, "/test/prefix/key2", "value2")
	require.NoError(t, err)

	// Wait for second event
	select {
	case event := <-ch:
		assert.Equal(t, Added, event.Type)
		assert.Equal(t, "/test/prefix/key2", event.Key)
		assert.Equal(t, "value2", string(event.Value))
	case <-timeout:
		t.Fatal("timeout waiting for second event")
	}

	// Test modification
	_, err = lw.etcdCli.Put(ctx, "/test/prefix/key1", "value1-modified")
	require.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, Modified, event.Type)
		assert.Equal(t, "/test/prefix/key1", event.Key)
		assert.Equal(t, "value1-modified", string(event.Value))
	case <-timeout:
		t.Fatal("timeout waiting for modification event")
	}

	// Test deletion
	_, err = lw.etcdCli.Delete(ctx, "/test/prefix/key1")
	require.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, Deleted, event.Type)
		assert.Equal(t, "/test/prefix/key1", event.Key)
	case <-timeout:
		t.Fatal("timeout waiting for deletion event")
	}
}
