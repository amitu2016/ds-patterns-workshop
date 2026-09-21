/*
Package listwatch provides functionality to list and watch resources using etcd.

ListWatch is a robust implementation for watching changes in etcd with features like:
  - Automatic reconnection with exponential backoff
  - Built-in metrics for monitoring
  - Configurable retry behavior
  - Efficient event delivery via channels

Basic Usage:

	// Create a ListWatch instance
	lw, err := listwatch.NewListWatch(
	    []string{"localhost:2379"},  // etcd endpoints
	    "/myapp/",                  // prefix to watch
	    listwatch.DefaultOptions(),   // default configuration
	    nil,                         // optional logger
	)

	// Start watching for changes
	eventCh, stopWatch, err := lw.ListAndWatch(ctx)
	if err != nil {
	    log.Fatal(err)
	}
	defer stopWatch()

	// Process events
	for event := range eventCh {
	    switch event.Type {
	    case listwatch.Added:
	        // Handle new item
	    case listwatch.Modified:
	        // Handle update
	    case listwatch.Deleted:
	        // Handle deletion
	    case listwatch.Error:
	        // Handle error
	    }
	}

Configuration:
The Options struct allows customizing:
  - DialTimeout: Timeout for etcd client connection
  - RetryInitialDelay: Initial delay for retry attempts
  - RetryMaxDelay: Maximum delay between retries
  - RetryMultiplier: Factor for exponential backoff
  - EventChannelBuffer: Size of the event channel buffer

Metrics:
The package exports Prometheus metrics for monitoring:
  - Event counts by type (add/modify/delete)
  - Connection state (connected/disconnected)
  - Watch session duration
  - Error counts by type

Error Handling:
Errors are handled in multiple ways:
1. Immediate errors are returned directly
2. Watch errors are sent as Error events
3. Connection failures trigger automatic reconnection
4. All errors are tracked via metrics
*/
package listwatch

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"kubelite/pkg/retry"
	"time"
)

const (
	// defaultErrorRetryAttempts is the number of attempts to send error events
	defaultErrorRetryAttempts = 3
	// defaultErrorRetryDelay is the delay between error event send attempts
	defaultErrorRetryDelay = 10 * time.Millisecond
)

// EventType defines the possible types of events.
type EventType string

const (
	// Added indicates a new object was created
	Added EventType = "ADDED"
	// Modified indicates an existing object was updated
	Modified EventType = "MODIFIED"
	// Deleted indicates an object was removed
	Deleted EventType = "DELETED"
	// Error indicates a problem occurred during watch/list operations
	Error EventType = "ERROR"
)

// Event represents a single event to a watched resource.
// It contains information about what changed and the associated data.
type Event struct {
	// Type indicates whether this is an Add, Modify, Delete, or Error event
	Type EventType
	// Key is the full key path of the resource that changed
	Key string
	// Value contains the current state of the resource
	// For delete events, this will be nil
	Value []byte
	// Prefix is the watch prefix that produced this event
	Prefix string
}

// validate checks if the Event is well-formed
func (e Event) validate() error {
	if e.Type == "" {
		return fmt.Errorf("event type cannot be empty")
	}
	if e.Key == "" && e.Type != Error {
		return fmt.Errorf("event key cannot be empty for non-error events")
	}
	if e.Prefix == "" {
		return fmt.Errorf("event prefix cannot be empty")
	}
	return nil
}

// Options configures the ListWatch behavior
type Options struct {
	DialTimeout        time.Duration
	RetryOpts          retry.Options
	EventChannelBuffer int
}

// DefaultOptions returns the default configuration options
func DefaultOptions() Options {
	return Options{
		DialTimeout:        5 * time.Second,
		RetryOpts:          retry.DefaultOptions(),
		EventChannelBuffer: 100,
	}
}

// tryToSendErrorEvent attempts to send an error event with retries
func (lw *ListWatch) tryToSendErrorEvent(ch chan<- Event, errMsg string, ctx context.Context) bool {
	err := retry.WithRetries(ctx, defaultErrorRetryAttempts, defaultErrorRetryDelay, func(ctx context.Context) error {
		select {
		case ch <- Event{Type: Error, Value: []byte(errMsg)}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("channel full")
		}
	})
	return err == nil
}

// sendEvent sends an event to the channel with context cancellation handling
func (lw *ListWatch) sendEvent(ctx context.Context, ch chan<- Event, event Event) error {
	if err := event.validate(); err != nil {
		lw.logger.Error("Invalid event", "error", err)
		return fmt.Errorf("invalid event: %v", err)
	}

	select {
	case ch <- event:
		lw.metrics.eventProcessed.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeEtcdClient safely closes and nullifies the etcd client
func (lw *ListWatch) closeEtcdClient() {
	if lw.etcdCli != nil {
		lw.etcdCli.Close()
		lw.etcdCli = nil
	}
}

// ensureConnected ensures we have a valid etcd client
func (lw *ListWatch) ensureConnected(ctx context.Context, ch chan<- Event) error {
	if lw.etcdCli != nil {
		return nil
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   lw.endpoints,
		DialTimeout: lw.opts.DialTimeout,
	})
	if err != nil {
		lw.logger.Error("Failed to create etcd client", "error", err)
		lw.metrics.connectionState.Set(0)
		lw.metrics.errorsByType.WithLabelValues("connection_failed").Inc()
		lw.tryToSendErrorEvent(ch, fmt.Sprintf("failed to create etcd client: %v", err), ctx)
		return err
	}

	lw.etcdCli = cli
	lw.metrics.connectionState.Set(1)
	return nil
}

// handleCleanup performs cleanup when the watch loop exits
func (lw *ListWatch) handleCleanup(ctx context.Context, ch chan Event, done chan struct{}) {
	lw.closeEtcdClient()
	lw.metrics.connectionState.Set(0)

	// Try to send context cancellation error
	if ctx.Err() != nil {
		lw.metrics.errorsByType.WithLabelValues("context_cancelled").Inc()
		lw.tryToSendErrorEvent(ch, fmt.Sprintf("context cancelled: %v", ctx.Err()), ctx)
	}

	close(ch)
	close(done)
	lw.logger.Info("ListWatch goroutine stopped")
}

// ListWatch knows how to list and watch a set of resources in etcd.
type ListWatch struct {
	endpoints   []string
	etcdCli     *clientv3.Client
	watchPrefix string
	opts        Options
	metrics     *metrics
	logger      Logger
}

// Logger interface for structured logging
type Logger interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// NewListWatch creates a new ListWatch for the given prefix.
func NewListWatch(endpoints []string, prefix string, opts Options, logger Logger) (*ListWatch, error) {
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: opts.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	return &ListWatch{
		endpoints:   endpoints,
		etcdCli:     cli,
		watchPrefix: prefix,
		opts:        opts,
		metrics:     newMetrics(),
		logger:      logger,
	}, nil
}

// listAndSendExisting lists and sends existing items to the channel
func (lw *ListWatch) listAndSendExisting(ctx context.Context, ch chan<- Event) error {
	start := time.Now()
	existing, err := lw.List(ctx)
	lw.metrics.listLatency.Observe(time.Since(start).Seconds())

	if err != nil {
		lw.logger.Error("Failed to list items", "error", err)
		lw.tryToSendErrorEvent(ch, fmt.Sprintf("failed to list items: %v", err), ctx)
		lw.closeEtcdClient()
		lw.metrics.retryCount.Inc()
		return err
	}

	for _, event := range existing {
		if err := lw.sendEvent(ctx, ch, event); err != nil {
			return err
		}
	}

	return nil
}

// List gets all keys with the configured prefix.
func (lw *ListWatch) List(ctx context.Context) ([]Event, error) {
	resp, err := lw.etcdCli.Get(ctx, lw.watchPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %v", err)
	}

	events := make([]Event, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		// If CreateRevision equals ModRevision, this is a new key
		eventType := Added
		if kv.CreateRevision != kv.ModRevision {
			eventType = Modified
		}

		events[i] = Event{
			Type:   eventType,
			Key:    string(kv.Key),
			Value:  kv.Value,
			Prefix: lw.watchPrefix,
		}
	}

	return events, nil
}

// handleWatchChannelClose handles the case when the watch channel closes unexpectedly
func (lw *ListWatch) handleWatchChannelClose(ctx context.Context, ch chan<- Event) error {
	lw.logger.Error("Watch channel closed unexpectedly")

	// Try multiple times to ensure error event is sent
	for i := 0; i < 3; i++ {
		select {
		case ch <- Event{Type: Error, Value: []byte("watch channel closed unexpectedly")}:
			lw.closeEtcdClient()
			return fmt.Errorf("watch channel closed")
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(10 * time.Millisecond) // Wait before retry
		}
	}

	// If we couldn't send the error event after retries, still close and return
	lw.closeEtcdClient()
	return fmt.Errorf("watch channel closed")
}

// watchAndForwardEvents starts a watch and forwards events to the channel
func (lw *ListWatch) watchAndForwardEvents(ctx context.Context, ch chan<- Event) error {
	watchCh, watchCancel, err := lw.Watch(ctx)
	if err != nil {
		lw.logger.Error("Failed to start watch", "error", err)
		lw.tryToSendErrorEvent(ch, fmt.Sprintf("failed to start watch: %v", err), ctx)
		lw.closeEtcdClient()
		return err
	}
	defer watchCancel()

	// Create a separate context for watch operations
	watchCtx, watchCtxCancel := context.WithCancel(ctx)
	defer watchCtxCancel()

	// Start a goroutine to monitor etcd client status
	go func() {
		<-watchCtx.Done()
		if ctx.Err() == nil { // Only send error if parent context is not done
			lw.tryToSendErrorEvent(ch, "etcd connection lost", ctx)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			lw.tryToSendErrorEvent(ch, "watch stopped: context cancelled", ctx)
			return ctx.Err()

		case event, ok := <-watchCh:
			if !ok {
				// Cancel watch context to trigger error event
				watchCtxCancel()
				return lw.handleWatchChannelClose(ctx, ch)
			}

			if err := lw.sendEvent(ctx, ch, event); err != nil {
				return err
			}
		}
	}
}

// Watch starts watching for changes on the configured prefix.
// It returns a channel that will receive events and a function to stop watching.
func (lw *ListWatch) Watch(ctx context.Context) (<-chan Event, func(), error) {
	start := time.Now()
	defer func() {
		lw.metrics.watchSessionDuration.Observe(time.Since(start).Seconds())
	}()

	// Get current revision
	resp, err := lw.etcdCli.Get(ctx, lw.watchPrefix, clientv3.WithPrefix())
	if err != nil {
		lw.metrics.errorsByType.WithLabelValues("get_revision_failed").Inc()
		return nil, nil, fmt.Errorf("failed to get current revision: %v", err)
	}

	// Create buffered channel to prevent blocking
	ch := make(chan Event, 100)

	// Create watch channel starting from next revision
	watchChan := lw.etcdCli.Watch(ctx, lw.watchPrefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))

	// Start goroutine to process watch events
	go func() {
		defer close(ch)

		for watchResp := range watchChan {
			if watchResp.Err() != nil {
				lw.metrics.errorsByType.WithLabelValues("watch_error").Inc()
				ch <- Event{Type: Error, Value: []byte(watchResp.Err().Error())}
				return
			}

			for _, event := range watchResp.Events {
				var eventType EventType
				switch event.Type {
				case clientv3.EventTypePut:
					// If CreateRevision equals ModRevision, this is a new key
					if event.Kv.CreateRevision == event.Kv.ModRevision {
						eventType = Added
					} else {
						eventType = Modified
					}
				case clientv3.EventTypeDelete:
					eventType = Deleted
				}

				event := Event{
					Type:   eventType,
					Key:    string(event.Kv.Key),
					Value:  event.Kv.Value,
					Prefix: lw.watchPrefix,
				}
				ch <- event
				lw.metrics.eventsByType.WithLabelValues(string(eventType)).Inc()
			}
		}
	}()

	// Return cancel function
	cancel := func() {
		if lw.etcdCli != nil {
			lw.etcdCli.Close()
		}
	}

	return ch, cancel, nil
}

// runListWatchLoop handles the main loop of listing and watching items
func (lw *ListWatch) runListWatchLoop(ctx context.Context, ch chan Event, done chan struct{}) {
	defer lw.handleCleanup(ctx, ch, done)

	for {
		if ctx.Err() != nil {
			return
		}

		err := retry.WithExponentialBackoff(ctx, lw.opts.RetryOpts, func(ctx context.Context) error {
			// Ensure we have a valid connection
			if err := lw.ensureConnected(ctx, ch); err != nil {
				return err
			}

			// List existing items
			if err := lw.listAndSendExisting(ctx, ch); err != nil {
				return err
			}

			// Watch for changes
			if err := lw.watchAndForwardEvents(ctx, ch); err != nil {
				return err
			}

			return nil
		})

		if err != nil && err != context.Canceled {
			lw.logger.Error("ListWatch loop failed", "error", err)
		}
	}
}

// ListAndWatch combines List and Watch operations with automatic retry on failures.
// It first lists all existing items and then starts watching for changes.
// If the watch operation fails, it will retry with exponential backoff.
func (lw *ListWatch) ListAndWatch(ctx context.Context) (<-chan Event, func(), error) {
	ch := make(chan Event, lw.opts.EventChannelBuffer)
	done := make(chan struct{})
	watchCtx, cancelWatch := context.WithCancel(ctx)

	go lw.runListWatchLoop(watchCtx, ch, done)

	// Return cancel function that ensures cleanup
	cancel := func() {
		cancelWatch()
		<-done // Wait for goroutine to finish cleanup
	}

	return ch, cancel, nil
}
