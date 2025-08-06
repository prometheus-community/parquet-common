// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"testing"
)

func TestNewBitmap(t *testing.T) {
	tests := []struct {
		name         string
		size         int
		expectedSize int
		expectedBits int
	}{
		{
			name:         "small bitmap",
			size:         10,
			expectedSize: 10,
			expectedBits: 1, // (10+63)/64 = 1
		},
		{
			name:         "exactly 64 bits",
			size:         64,
			expectedSize: 64,
			expectedBits: 1, // (64+63)/64 = 1
		},
		{
			name:         "larger bitmap",
			size:         128,
			expectedSize: 128,
			expectedBits: 2, // (128+63)/64 = 2
		},
		{
			name:         "zero size",
			size:         0,
			expectedSize: 0,
			expectedBits: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bm := NewBitmap(tt.size)
			if bm.size != tt.expectedSize {
				t.Errorf("expected size %d, got %d", tt.expectedSize, bm.size)
			}
			if len(bm.bits) != tt.expectedBits {
				t.Errorf("expected bits length %d, got %d", tt.expectedBits, len(bm.bits))
			}
		})
	}
}

func TestBitmap_Set(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		position int
		expected bool
	}{
		{
			name:     "set first bit",
			size:     64,
			position: 0,
			expected: true,
		},
		{
			name:     "set middle bit",
			size:     64,
			position: 32,
			expected: true,
		},
		{
			name:     "set last bit",
			size:     64,
			position: 63,
			expected: true,
		},
		{
			name:     "set bit in second uint64",
			size:     128,
			position: 65,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bm := NewBitmap(tt.size)
			bm.Set(tt.position)
			if bm.Get(tt.position) != tt.expected {
				t.Errorf("expected bit at position %d to be %v", tt.position, tt.expected)
			}
		})
	}
}

func TestBitmap_SetOutOfBounds(t *testing.T) {
	bm := NewBitmap(64)

	// Test negative index
	bm.Set(-1)
	// Should not panic and should not affect any bits

	// Test index equal to size
	bm.Set(64)
	// Should not panic and should not affect any bits

	// Test index greater than size
	bm.Set(100)
	// Should not panic and should not affect any bits

	// Verify no bits are set
	for i := 0; i < 64; i++ {
		if bm.Get(i) {
			t.Errorf("unexpected bit set at position %d after out-of-bounds operations", i)
		}
	}
}

func TestBitmap_Clear(t *testing.T) {
	bm := NewBitmap(64)

	// Set some bits first
	positions := []int{0, 15, 31, 63}
	for _, pos := range positions {
		bm.Set(pos)
	}

	// Verify they are set
	for _, pos := range positions {
		if !bm.Get(pos) {
			t.Errorf("bit at position %d should be set", pos)
		}
	}

	// Clear some bits
	clearPositions := []int{0, 31}
	for _, pos := range clearPositions {
		bm.Clear(pos)
	}

	// Verify cleared bits are unset
	for _, pos := range clearPositions {
		if bm.Get(pos) {
			t.Errorf("bit at position %d should be cleared", pos)
		}
	}

	// Verify other bits are still set
	stillSet := []int{15, 63}
	for _, pos := range stillSet {
		if !bm.Get(pos) {
			t.Errorf("bit at position %d should still be set", pos)
		}
	}
}

func TestBitmap_ClearOutOfBounds(t *testing.T) {
	bm := NewBitmap(64)

	// Set all bits first
	for i := 0; i < 64; i++ {
		bm.Set(i)
	}

	// Test clearing out of bounds
	bm.Clear(-1)
	bm.Clear(64)
	bm.Clear(100)

	// Verify all bits are still set
	for i := 0; i < 64; i++ {
		if !bm.Get(i) {
			t.Errorf("bit at position %d should still be set after out-of-bounds clear operations", i)
		}
	}
}

func TestBitmap_Get(t *testing.T) {
	bm := NewBitmap(128)

	// Initially all bits should be unset
	for i := 0; i < 128; i++ {
		if bm.Get(i) {
			t.Errorf("bit at position %d should initially be unset", i)
		}
	}

	// Set some specific bits
	setBits := []int{0, 1, 63, 64, 127}
	for _, pos := range setBits {
		bm.Set(pos)
	}

	// Test getting set bits
	for _, pos := range setBits {
		if !bm.Get(pos) {
			t.Errorf("bit at position %d should be set", pos)
		}
	}

	// Test getting unset bits
	unsetBits := []int{2, 32, 62, 65, 126}
	for _, pos := range unsetBits {
		if bm.Get(pos) {
			t.Errorf("bit at position %d should be unset", pos)
		}
	}
}

func TestBitmap_GetOutOfBounds(t *testing.T) {
	bm := NewBitmap(64)

	// Test getting out of bounds bits
	outOfBoundsCases := []int{-1, 64, 100}
	for _, pos := range outOfBoundsCases {
		if bm.Get(pos) {
			t.Errorf("out-of-bounds position %d should return false", pos)
		}
	}
}

func TestBitmap_MultipleBitOperations(t *testing.T) {
	// Use 192 (3*64) to ensure we have exactly 3 uint64s allocated
	bm := NewBitmap(192)

	// Set every 3rd bit
	for i := 0; i < 192; i += 3 {
		bm.Set(i)
	}

	// Verify every 3rd bit is set
	for i := 0; i < 192; i++ {
		expected := i%3 == 0
		if bm.Get(i) != expected {
			t.Errorf("bit at position %d: expected %v, got %v", i, expected, bm.Get(i))
		}
	}

	// Clear every 6th bit (which should be set)
	for i := 0; i < 192; i += 6 {
		bm.Clear(i)
	}

	// Verify the pattern
	for i := 0; i < 192; i++ {
		var expected bool
		if i%3 == 0 && i%6 != 0 {
			expected = true
		}
		if bm.Get(i) != expected {
			t.Errorf("after clearing, bit at position %d: expected %v, got %v", i, expected, bm.Get(i))
		}
	}
}

func TestBitmap_AllocationBehavior(t *testing.T) {
	// Test the corrected allocation behavior where (size+63)/64 is used
	// This ensures sufficient space is allocated for all valid indices
	tests := []struct {
		name         string
		size         int
		expectedBits int
		maxValidIdx  int
	}{
		{
			name:         "size 63 allocates 1 uint64",
			size:         63,
			expectedBits: 1,
			maxValidIdx:  62,
		},
		{
			name:         "size 64 allocates 1 uint64",
			size:         64,
			expectedBits: 1,
			maxValidIdx:  63,
		},
		{
			name:         "size 127 allocates 2 uint64s",
			size:         127,
			expectedBits: 2,
			maxValidIdx:  126,
		},
		{
			name:         "size 128 allocates 2 uint64s",
			size:         128,
			expectedBits: 2,
			maxValidIdx:  127,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bm := NewBitmap(tt.size)
			if len(bm.bits) != tt.expectedBits {
				t.Errorf("expected %d uint64s allocated, got %d", tt.expectedBits, len(bm.bits))
			}

			// Test that operations within the valid range work
			bm.Set(tt.maxValidIdx)
			if !bm.Get(tt.maxValidIdx) {
				t.Errorf("should be able to set bit at index %d", tt.maxValidIdx)
			}
		})
	}
}

func TestBitmap_NonMultipleOf64Sizes(t *testing.T) {
	// Test that sizes that aren't multiples of 64 work correctly now
	tests := []int{1, 10, 63, 65, 100, 127, 200}

	for _, size := range tests {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			bm := NewBitmap(size)

			// Test setting the last valid bit
			lastBit := size - 1
			bm.Set(lastBit)
			if !bm.Get(lastBit) {
				t.Errorf("should be able to set bit at last valid index %d", lastBit)
			}

			// Test that out-of-bounds operations are still handled
			bm.Set(size) // Should not panic
			if bm.Get(size) {
				t.Errorf("out-of-bounds get should return false")
			}
		})
	}
}

func TestBitmap_CrossBoundaryOperations(t *testing.T) {
	bm := NewBitmap(128)

	// Test operations around the 64-bit boundary
	boundaryPositions := []int{62, 63, 64, 65}

	// Set bits around boundary
	for _, pos := range boundaryPositions {
		bm.Set(pos)
	}

	// Verify all are set
	for _, pos := range boundaryPositions {
		if !bm.Get(pos) {
			t.Errorf("bit at boundary position %d should be set", pos)
		}
	}

	// Clear some and verify
	bm.Clear(63)
	bm.Clear(64)

	if bm.Get(63) {
		t.Error("bit 63 should be cleared")
	}
	if bm.Get(64) {
		t.Error("bit 64 should be cleared")
	}
	if !bm.Get(62) {
		t.Error("bit 62 should still be set")
	}
	if !bm.Get(65) {
		t.Error("bit 65 should still be set")
	}
}
