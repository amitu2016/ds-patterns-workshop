package retry

import (
	"context"
	"time"
)

// Options configures the retry behavior
type Options struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultOptions returns the default retry configuration
func DefaultOptions() Options {
	return Options{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
	}
}

// WithExponentialBackoff executes the given operation with exponential backoff
func WithExponentialBackoff(ctx context.Context, opts Options, operation func(context.Context) error) error {
	currentDelay := opts.InitialDelay

	for {
		err := operation(ctx)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(currentDelay):
			// Calculate next delay with exponential backoff
			nextDelay := time.Duration(float64(currentDelay) * opts.Multiplier)
			if nextDelay > opts.MaxDelay {
				nextDelay = opts.MaxDelay
			}
			currentDelay = nextDelay
		}
	}
}

// WithRetries attempts to execute an operation with a fixed number of retries
func WithRetries(ctx context.Context, attempts int, delay time.Duration, operation func(context.Context) error) error {
	for i := 0; i < attempts; i++ {
		err := operation(ctx)
		if err == nil {
			return nil
		}

		if i == attempts-1 {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}
	return nil
}
