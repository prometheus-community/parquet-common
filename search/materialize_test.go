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

package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

func TestMaterializeE2E(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := util.DefaultTestConfig()
	data := util.GenerateTestData(t, st, ctx, cfg)

	// Convert to Parquet
	shard := convertToParquet(t, ctx, bkt, data, st.Head())

	t.Run("QueryByUniqueLabel", func(t *testing.T) {
		eq := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))
		found := query(t, data.MinTime, data.MaxTime, shard, eq)
		require.Len(t, found, cfg.TotalMetricNames)

		for _, series := range found {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.SeriesHash, series.Labels().Hash())
		}
	})

	t.Run("QueryByMetricName", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("metric_%d", rand.Int()%cfg.TotalMetricNames)
			eq := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf(name))

			found := query(t, data.MinTime, data.MaxTime, shard, eq)
			require.Len(t, found, cfg.MetricsPerMetricName, fmt.Sprintf("metric_%d", i))

			for _, series := range found {
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.SeriesHash, series.Labels().Hash())

				totalSamples := 0
				ci := series.Iterator(nil)
				for ci.Next() {
					si := ci.At().Chunk.Iterator(nil)
					for si.Next() != chunkenc.ValNone {
						totalSamples++
					}
				}
				require.Equal(t, totalSamples, cfg.NumberOfSamples)
			}
		}
	})

	t.Run("QueryByTimeRange", func(t *testing.T) {
		colDuration := time.Hour
		c1 := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf("metric_0"))
		c2 := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))

		// Test first column only
		found := query(t, data.MinTime, data.MinTime+colDuration.Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, found[0].(*concreteChunksSeries).chks, 1)

		// Test first two columns
		found = query(t, data.MinTime, data.MinTime+(2*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, found[0].(*concreteChunksSeries).chks, 2)

		// Query outside the range
		found = query(t, data.MinTime+(9*colDuration).Milliseconds(), data.MinTime+(10*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 0)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)
		rr := []RowRange{{From: int64(0), Count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.ErrorContains(t, err, "context canceled")
	})

	t.Run("Should not race when multiples download multiples page in parallel", func(t *testing.T) {
		bucketOpener := storage.NewParquetBucketOpener(bkt)
		shard, err := storage.NewParquetShardOpener(
			ctx, "shard", bucketOpener, bucketOpener, 0, storage.WithPageMaxGapSize(-1),
		)
		require.NoError(t, err)
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)
		rr := []RowRange{{From: int64(0), Count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
	})

	t.Run("RowCountQuota", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

		// Test with limited row count quota
		limitedRowCountQuota := NewQuota(10) // Only allow 10 rows
		m, err := NewMaterializer(s, d, shard, 10, limitedRowCountQuota, UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		// Try to materialize more rows than quota allows
		rr := []RowRange{{From: int64(0), Count: 20}} // 20 rows
		_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would fetch too many rows")
		require.True(t, IsResourceExhausted(err))

		// Test with sufficient quota
		sufficientRowCountQuota := NewQuota(1000) // Allow 1000 rows
		m, err = NewMaterializer(s, d, shard, 10, sufficientRowCountQuota, UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		rr = []RowRange{{From: int64(0), Count: 50}} // 50 rows
		series, err := m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
		require.NotEmpty(t, series)
	})

	t.Run("ChunkBytesQuota", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

		// Test with limited chunk bytes quota
		limitedChunkBytesQuota := NewQuota(100) // Only allow 100 bytes
		m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), limitedChunkBytesQuota, UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		// Try to materialize chunks that exceed the quota
		rr := []RowRange{{From: int64(0), Count: 100}} // Large range to trigger chunk reading
		_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would fetch too many chunk bytes")
		require.True(t, IsResourceExhausted(err))

		// Test with sufficient quota
		sufficientChunkBytesQuota := NewQuota(1000000) // Allow 1MB
		m, err = NewMaterializer(s, d, shard, 10, UnlimitedQuota(), sufficientChunkBytesQuota, UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		rr = []RowRange{{From: int64(0), Count: 10}} // Small range
		series, err := m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
		require.NotEmpty(t, series)
	})

	t.Run("DataBytesQuota", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

		// Test with limited data bytes quota
		limitedDataBytesQuota := NewQuota(100) // Only allow 100 bytes
		m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), limitedDataBytesQuota, NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		// Try to materialize data that exceeds the quota
		rr := []RowRange{{From: int64(0), Count: 100}} // Large range to trigger data reading
		_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would fetch too many data bytes")
		require.True(t, IsResourceExhausted(err))

		// Test with sufficient quota
		sufficientDataBytesQuota := NewQuota(1000000) // Allow 1MB
		m, err = NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), sufficientDataBytesQuota, NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		require.NoError(t, err)

		rr = []RowRange{{From: int64(0), Count: 10}} // Small range
		series, err := m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
		require.NotEmpty(t, series)
	})
}

func convertToParquet(t testing.TB, ctx context.Context, bkt objstore.Bucket, data util.TestData, h convert.Convertible, opts ...storage.FileOption) storage.ParquetShard {
	colDuration := time.Hour
	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.MinTime,
		data.MaxTime,
		[]convert.Convertible{h},
		convert.WithName("shard"),
		convert.WithColDuration(colDuration), // let's force more than 1 data col
		convert.WithRowGroupSize(500),
		convert.WithPageBufferSize(300), // force creating multiples pages
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(
		ctx, "shard", bucketOpener, bucketOpener, 0,
	)
	require.NoError(t, err)

	return shard
}

func query(t *testing.T, mint, maxt int64, shard storage.ParquetShard, constraints ...Constraint) []prom_storage.ChunkSeries {
	ctx := context.Background()
	for _, c := range constraints {
		require.NoError(t, c.init(shard.LabelsFile()))
	}

	s, err := shard.TSDBSchema()
	require.NoError(t, err)
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
	require.NoError(t, err)

	results := make([]prom_storage.ChunkSeries, 0, 100)
	for i := range shard.LabelsFile().RowGroups() {
		rr, err := Filter(context.Background(), shard, i, constraints...)
		total := int64(0)
		for _, r := range rr {
			total += r.Count
		}
		require.NoError(t, err)

		seriesSetIter, err := m.Materialize(ctx, nil, i, mint, maxt, false, rr)
		require.NoError(t, err)

		for seriesSetIter.Next() {
			seriesIter := seriesSetIter.At()
			concreteSeriesFromIter := &concreteChunksSeries{
				lbls: seriesIter.Labels(),
			}
			seriesChunksIter := seriesIter.Iterator(nil)
			for seriesChunksIter.Next() {
				concreteSeriesFromIter.chks = append(concreteSeriesFromIter.chks, seriesChunksIter.At())
			}
			results = append(results, concreteSeriesFromIter)
		}
	}
	return results
}

func TestFilterSeries(t *testing.T) {
	ctx := context.Background()

	// Create sample labels for testing
	sampleLabels := [][]labels.Label{
		{
			{Name: "__name__", Value: "metric_1"},
			{Name: "instance", Value: "server1"},
			{Name: "job", Value: "web"},
		},
		{
			{Name: "__name__", Value: "metric_2"},
			{Name: "instance", Value: "server2"},
			{Name: "job", Value: "db"},
		},
		{
			{Name: "__name__", Value: "metric_3"},
			{Name: "instance", Value: "server1"},
			{Name: "job", Value: "web"},
		},
		{
			{Name: "__name__", Value: "metric_4"},
			{Name: "instance", Value: "server3"},
			{Name: "job", Value: "cache"},
		},
	}

	// Use non-sequential row ranges to test proper row mapping
	sampleRowRanges := []RowRange{
		{From: 10, Count: 1},
		{From: 25, Count: 1},
		{From: 50, Count: 1},
		{From: 100, Count: 1},
	}

	// Create a mock materializer
	materializer := &Materializer{}

	t.Run("NoFilteringEnabled", func(t *testing.T) {
		// Test when filtering is disabled (callback returns false)
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return nil, false
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return all series without filtering
		require.Len(t, results, len(sampleLabels))
		require.Equal(t, sampleRowRanges, filteredRR)

		// Verify all labels are preserved
		for i, result := range results {
			expectedLabels := labels.New(sampleLabels[i]...)
			require.Equal(t, expectedLabels, result)
		}
	})

	t.Run("AcceptAllFilter", func(t *testing.T) {
		// Test filter that accepts all series
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &acceptAllFilter{}, true
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return all series
		require.Len(t, results, len(sampleLabels))
		require.Equal(t, sampleRowRanges, filteredRR)

		// Verify all labels are preserved
		for i, result := range results {
			expectedLabels := labels.New(sampleLabels[i]...)
			require.Equal(t, expectedLabels, result)
		}
	})

	t.Run("RejectAllFilter", func(t *testing.T) {
		// Test filter that rejects all series
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &rejectAllFilter{}, true
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return no series
		require.Len(t, results, 0)
		require.Len(t, filteredRR, 0)
	})

	t.Run("SelectiveFilter", func(t *testing.T) {
		// Test filter that only accepts series with job="web"
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &jobWebFilter{}, true
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		// Should return only series with job="web" (indices 0 and 2)
		require.Len(t, results, 2)
		require.Len(t, filteredRR, 2)

		// Verify correct series are returned
		require.Equal(t, "metric_1", results[0].Get("__name__"))
		require.Equal(t, "web", results[0].Get("job"))
		require.Equal(t, "metric_3", results[1].Get("__name__"))
		require.Equal(t, "web", results[1].Get("job"))

		// Verify row ranges map to the actual row positions from input
		expectedRR := []RowRange{
			{From: 10, Count: 1},
			{From: 50, Count: 1},
		}
		require.Equal(t, expectedRR, filteredRR)
	})

	t.Run("ContiguousRangeMerging", func(t *testing.T) {
		// Test with contiguous row ranges that should be merged
		contiguousRowRanges := []RowRange{
			{From: 100, Count: 1},
			{From: 101, Count: 1},
			{From: 102, Count: 1},
			{From: 200, Count: 1},
		}

		// Filter that accepts first two series (should create contiguous range)
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &firstTwoFilter{}, true
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, sampleLabels, contiguousRowRanges)

		// Should return first two series
		require.Len(t, results, 2)
		require.Len(t, filteredRR, 1) // Should be merged into one contiguous range

		// Verify correct series are returned
		require.Equal(t, "metric_1", results[0].Get("__name__"))
		require.Equal(t, "metric_2", results[1].Get("__name__"))

		// Verify contiguous ranges are merged
		expectedRR := []RowRange{
			{From: 100, Count: 2}, // Merged range covering rows 100-101
		}
		require.Equal(t, expectedRR, filteredRR)
	})

	t.Run("EmptyInput", func(t *testing.T) {
		// Test with empty input
		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return &acceptAllFilter{}, true
		}

		results, filteredRR := materializer.filterSeriesLabels(ctx, nil, nil, nil)

		require.Len(t, results, 0)
		require.Len(t, filteredRR, 0)
	})

	t.Run("FilterCloseIsCalled", func(t *testing.T) {
		// Test that Close() is called on the filter
		closeCalled := false
		filter := &trackingFilter{
			acceptAll:   true,
			closeCalled: &closeCalled,
		}

		materializer.materializedLabelsFilterCallback = func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
			return filter, true
		}

		_, _ = materializer.filterSeriesLabels(ctx, nil, sampleLabels, sampleRowRanges)

		require.True(t, closeCalled, "Filter Close() should have been called")
	})
}

// Test filter implementations

type acceptAllFilter struct{}

func (f *acceptAllFilter) Filter(ls labels.Labels) bool {
	return true
}

func (f *acceptAllFilter) Close() {}

type rejectAllFilter struct{}

func (f *rejectAllFilter) Filter(ls labels.Labels) bool {
	return false
}

func (f *rejectAllFilter) Close() {}

type jobWebFilter struct{}

func (f *jobWebFilter) Filter(ls labels.Labels) bool {
	return ls.Get("job") == "web"
}

func (f *jobWebFilter) Close() {}

type firstTwoFilter struct{}

func (f *firstTwoFilter) Filter(ls labels.Labels) bool {
	name := ls.Get("__name__")
	return name == "metric_1" || name == "metric_2"
}

func (f *firstTwoFilter) Close() {}

type trackingFilter struct {
	acceptAll   bool
	closeCalled *bool
}

func (f *trackingFilter) Filter(ls labels.Labels) bool {
	return f.acceptAll
}

func (f *trackingFilter) Close() {
	*f.closeCalled = true
}
