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

package convert

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
)

func Test_Convert_TSDB(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		dataColDuration        time.Duration
		step                   time.Duration
		numberOfSamples        int
		expectedNumberOfChunks int
		expectedPointsPerChunk int
	}{
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        3,
			expectedNumberOfChunks: 3,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        48,
			expectedNumberOfChunks: 48,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        10,
			expectedNumberOfChunks: 1,
			expectedPointsPerChunk: 10,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        32,
			expectedNumberOfChunks: 2,
			expectedPointsPerChunk: 16,
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("dataColDurationMs:%v,step:%v,numberOfSamples:%v", tt.dataColDuration.Hours(), tt.step.Seconds(), tt.numberOfSamples), func(t *testing.T) {
			st := teststorage.New(t)
			t.Cleanup(func() { _ = st.Close() })

			app := st.Appender(ctx)
			seriesHash := make(map[uint64]struct{})
			for i := 0; i != 1_000; i++ {
				for j := 0; j < tt.numberOfSamples; j++ {
					lbls := labels.FromStrings("__name__", "foo", "bar", fmt.Sprintf("%d", 2*i))
					seriesHash[lbls.Hash()] = struct{}{}
					_, err := app.Append(0, lbls, (tt.step * time.Duration(j)).Milliseconds(), float64(i))
					require.NoError(t, err)
				}
			}

			require.NoError(t, app.Commit())

			h := st.Head()
			rr, err := newTsdbRowReader(ctx, h.MinTime(), h.MaxTime(), tt.dataColDuration.Milliseconds(), []Convertible{h})
			require.NoError(t, err)

			defer func() { _ = rr.Close() }()

			buf := make([]parquet.Row, 100)
			chunksDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
			total := 0

			for {
				n, _ := rr.ReadRows(buf)
				if n == 0 {
					break
				}

				total += n
				series, chunks, err := rowToSeries(rr.schema, chunksDecoder, buf[:n])
				require.NoError(t, err)
				require.Len(t, series, n)
				for i, s := range series {
					require.Contains(t, seriesHash, s.Hash())
					require.Len(t, chunks[i], tt.expectedNumberOfChunks)
					totalSamples := 0
					for _, c := range chunks[i] {
						require.Equal(t, tt.expectedPointsPerChunk, c.Chunk.NumSamples())
						totalSamples += c.Chunk.NumSamples()
					}
					require.Equal(t, tt.numberOfSamples, totalSamples)
				}
			}

			require.Equal(t, st.DB.Head().NumSeries(), uint64(total))
		})
	}
}

func Test_CreateParquetWithReducedTimestampSamples(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	app := st.Appender(ctx)

	// 240 samples * 30 seconds = 2 hours
	step := (30 * time.Second).Milliseconds()
	for i := 0; i < 240; i++ {
		_, err := app.Append(0, labels.FromStrings("__name__", "foo"), int64(i)*step, float64(i))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())

	h := st.Head()
	mint, maxt := (time.Minute * 30).Milliseconds(), (time.Minute*90).Milliseconds()-1
	rr, err := newTsdbRowReader(ctx, mint, maxt, (time.Minute * 10).Milliseconds(), []Convertible{h})
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()
	// 6 data cols with 10 min duration
	require.Len(t, rr.schema.DataColsIndexes, 6)

	chunksDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	buf := make([]parquet.Row, 100)
	n, _ := rr.ReadRows(buf)
	require.Equal(t, 1, n)

	series, chunks, err := rowToSeries(rr.schema, chunksDecoder, buf[:n])
	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Len(t, chunks, 1)
	require.Equal(t, labels.FromStrings("__name__", "foo").Hash(), series[0].Hash())

	totalSamples := 0
	for _, c := range chunks[0] {
		totalSamples += c.Chunk.NumSamples()
		require.LessOrEqual(t, c.MaxTime, maxt)
		require.GreaterOrEqual(t, c.MinTime, mint)
	}
	require.Equal(t, 120, totalSamples)
}

func Test_SortedLabels(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })
	st2 := teststorage.New(t)
	t.Cleanup(func() { _ = st2.Close() })

	app := st.Appender(ctx)
	app2 := st2.Appender(ctx)

	totalSeries := 0
	// Some very random series
	for i := 0; i < 240; i++ {
		_, err := app.Append(0, labels.FromStrings(
			labels.MetricName, fmt.Sprintf("%v", rand.Int31()),
			"type", "app",
			"zzz", fmt.Sprintf("%v", rand.Int31()),
			"i", fmt.Sprintf("%v", i),
		), 10, float64(i))
		require.NoError(t, err)
		totalSeries++
	}

	// Less random series making sure some metric names have more than 1 foo value
	for i := 0; i < 240; i++ {
		_, err := app2.Append(0, labels.FromStrings(
			labels.MetricName, fmt.Sprintf("%v", rand.Int31()%20),
			"type", "app2",
			"zzz", fmt.Sprintf("%v", rand.Int31()),
			"i", fmt.Sprintf("%v", i),
		), 10, float64(i))
		require.NoError(t, err)
		totalSeries++
	}

	// Lets create some common series on both blocks
	for i := 0; i < 240; i++ {
		lbls := labels.FromStrings(
			labels.MetricName, fmt.Sprintf("%v", rand.Int31()%20),
			"type", "duplicated",
			"zzz", fmt.Sprintf("%v", rand.Int31()),
			"i", fmt.Sprintf("%v", i),
		)
		_, err := app.Append(0, lbls, 10, float64(i))
		require.NoError(t, err)
		_, err = app2.Append(0, lbls, 11, float64(i+1))
		require.NoError(t, err)
		totalSeries++
	}

	require.NoError(t, app.Commit())
	require.NoError(t, app2.Commit())

	h := st.Head()
	h2 := st2.Head()
	// lets sort first by `zzz` as its not the default sorting on TSDB
	rr, err := newTsdbRowReader(ctx, 0, time.Minute.Milliseconds(), (time.Minute * 10).Milliseconds(), []Convertible{h, h2}, "zzz", labels.MetricName)
	require.NoError(t, err)

	buf := make([]parquet.Row, h.NumSeries()+h2.NumSeries())
	n, _ := rr.ReadRows(buf)
	require.Equal(t, totalSeries, n)

	series, chunks, err := rowToSeries(rr.schema, schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()), buf[:n])
	require.NoError(t, err)
	require.Len(t, series, n)

	for i := 0; i < len(series)-1; i++ {
		require.LessOrEqual(t, series[i].Get("zzz"), series[i+1].Get("zzz"))
		if series[i].Get("zzz") == series[i+1].Get("zzz") {
			require.LessOrEqual(t, series[i].Get(labels.MetricName), series[i+1].Get(labels.MetricName))
		}
		require.Len(t, chunks[i], 1)
		st := chunks[i][0].Chunk.Iterator(nil)
		expectedSamples := 1
		if series[i].Get("type") == "duplicated" {
			expectedSamples++
		}
		totalSamples := 0

		for st.Next() != chunkenc.ValNone {
			totalSamples++
		}

		require.Equal(t, expectedSamples, totalSamples)

		require.NoError(t, st.Err())
	}
}

func rowToSeries(s *schema.TSDBSchema, dec *schema.PrometheusParquetChunksDecoder, rows []parquet.Row) ([]labels.Labels, [][]chunks.Meta, error) {
	cols := s.Schema.Columns()
	b := labels.NewScratchBuilder(10)
	series := make([]labels.Labels, len(rows))
	chunksMetas := make([][]chunks.Meta, len(rows))

	for i, row := range rows {
		b.Reset()
		for colIdx, colVal := range row {
			col := cols[colIdx][0]
			label, ok := schema.ExtractLabelFromColumn(col)
			if ok {
				b.Add(label, colVal.String())
			}

			if schema.IsDataColumn(col) && dec != nil {
				c, err := dec.Decode(colVal.ByteArray(), 0, math.MaxInt64)
				if err != nil {
					return nil, nil, err
				}
				chunksMetas[i] = append(chunksMetas[i], c...)
			}
		}
		series[i] = b.Labels()
	}

	return series, chunksMetas, nil
}
