package schema

import (
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_DataCols(t *testing.T) {
	testCases := []struct {
		mint, maxt, dataColDuration int64
		expected                    int
	}{
		{
			mint:            0,
			maxt:            10*time.Hour.Milliseconds() - 1,
			dataColDuration: time.Hour.Milliseconds(),
			expected:        10,
		},
		{
			mint:            0,
			maxt:            10*time.Hour.Milliseconds() - 1,
			dataColDuration: 30 * time.Minute.Milliseconds(),
			expected:        20,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d,%d,%d", tc.mint, tc.maxt, tc.dataColDuration), func(t *testing.T) {
			b := NewBuilder(tc.mint, tc.maxt, tc.dataColDuration)
			s, err := b.Build()
			require.NoError(t, err)
			require.Len(t, s.DataColsIndexes, tc.expected)
		})
	}
}

func Test_LabelCols(t *testing.T) {
	b := NewBuilder(0, time.Hour.Milliseconds(), time.Hour.Milliseconds())
	b.AddLabelNameColumn("test", labels.MetricName)
	s, err := b.Build()
	require.NoError(t, err)
	_, ok := s.Schema.Lookup(LabelToColumn("test"))
	require.True(t, ok)
	_, ok = s.Schema.Lookup(LabelToColumn(labels.MetricName))
	require.True(t, ok)
	require.Equal(t, b.metadata[DataColSizeMd], fmt.Sprintf("%v", time.Hour.Milliseconds()))
}
