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
		m, err := NewMaterializer(s, d, shard, 10)
		require.NoError(t, err)
		rr := []RowRange{{from: int64(0), count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err = m.Materialize(ctx, 0, data.MinTime, data.MaxTime, false, rr)
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
		m, err := NewMaterializer(s, d, shard, 10)
		require.NoError(t, err)
		rr := []RowRange{{from: int64(0), count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		_, err = m.Materialize(ctx, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
	})
}

func convertToParquet(t *testing.T, ctx context.Context, bkt *filesystem.Bucket, data util.TestData, h convert.Convertible, opts ...storage.ShardOption) storage.ParquetShard {
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
		require.NoError(t, c.init(shard))
	}

	s, err := shard.TSDBSchema()
	require.NoError(t, err)
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	m, err := NewMaterializer(s, d, shard, 10)
	require.NoError(t, err)

	found := make([]prom_storage.ChunkSeries, 0, 100)
	for i := range shard.LabelsFile().RowGroups() {
		rr, err := Filter(context.Background(), shard, i, constraints...)
		total := int64(0)
		for _, r := range rr {
			total += r.count
		}
		require.NoError(t, err)
		series, err := m.Materialize(ctx, i, mint, maxt, false, rr)
		require.NoError(t, err)
		found = append(found, series...)
	}
	return found
}
