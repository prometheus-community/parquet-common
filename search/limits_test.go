package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsResourceExhausted(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "resource exhausted error",
			err:      &resourceExhausted{used: 100},
			expected: true,
		},
		{
			name:     "wrapped resource exhausted error",
			err:      fmt.Errorf("wrapped: %w", &resourceExhausted{used: 50}),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsResourceExhausted(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestQuota_Reserve(t *testing.T) {
	tests := []struct {
		name         string
		quotaLimit   int64
		reservations []int64
		expectError  bool
	}{
		{
			name:         "reserve within limit",
			quotaLimit:   100,
			reservations: []int64{30, 40, 20},
			expectError:  false,
		},
		{
			name:         "reserve exact limit",
			quotaLimit:   100,
			reservations: []int64{100},
			expectError:  false,
		},
		{
			name:         "reserve beyond limit",
			quotaLimit:   100,
			reservations: []int64{50, 60},
			expectError:  true,
		},
		{
			name:         "reserve zero amount",
			quotaLimit:   100,
			reservations: []int64{0},
			expectError:  false,
		},
		{
			name:         "reserve negative amount",
			quotaLimit:   100,
			reservations: []int64{-10},
			expectError:  false, // This should work as it increases available quota
		},
		{
			name:         "unlimited quota",
			quotaLimit:   0,
			reservations: []int64{1000, 2000, 3000},
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota := NewQuota(tt.quotaLimit)
			var lastErr error

			for _, amount := range tt.reservations {
				err := quota.Reserve(amount)
				if err != nil {
					lastErr = err
					require.True(t, IsResourceExhausted(err), "Expected resource exhausted error")
					break
				}
			}

			if tt.expectError {
				require.Error(t, lastErr, "Expected error but got none")
			} else {
				require.NoError(t, lastErr, "Unexpected error")
			}
		})
	}
}

func TestQuota_ConcurrentReserve(t *testing.T) {
	quota := NewQuota(1000)
	const numGoroutines = 10
	const reservationAmount = 100

	// Use a channel to collect errors from goroutines
	errChan := make(chan error, numGoroutines)

	// Start multiple goroutines trying to reserve simultaneously
	for i := 0; i < numGoroutines; i++ {
		go func() {
			err := quota.Reserve(reservationAmount)
			errChan <- err
		}()
	}

	// Collect all errors
	var errors []error
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		errors = append(errors, err)
	}

	// Should have exactly 10 successful reservations (1000 total)
	successCount := 0
	exhaustedCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		} else if IsResourceExhausted(err) {
			exhaustedCount++
		} else {
			require.Fail(t, "Unexpected error type", "error: %v", err)
		}
	}

	expectedSuccess := 10 // 1000 / 100 = 10
	require.Equal(t, expectedSuccess, successCount, "Expected %d successful reservations", expectedSuccess)
	require.Equal(t, 0, exhaustedCount, "Expected 0 exhausted errors")

	// Next reservation should fail
	err := quota.Reserve(1)
	require.Error(t, err, "Expected error when reserving beyond quota limit")
	require.True(t, IsResourceExhausted(err), "Expected resource exhausted error")
}
