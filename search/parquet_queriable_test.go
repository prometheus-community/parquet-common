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

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
)

func TestQueryable(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := defaultTestConfig()
	data := generateTestData(t, st, ctx, cfg)

	ir, err := st.Head().Index()
	require.NoError(t, err)

	// Convert to Parquet
	lf, cf := convertToParquet(t, ctx, bkt, data, st.Head())

	t.Run("QueryByUniqueLabel", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
		sFound := queryWithQueryable(t, data.minTime, data.maxTime, lf, cf, nil, matchers...)
		totalFound := 0
		for _, series := range sFound {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
			totalFound++
		}
		require.Equal(t, cfg.totalMetricNames, totalFound)
	})

	t.Run("QueryByMetricName", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("metric_%d", rand.Int()%cfg.totalMetricNames)
			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}
			sFound := queryWithQueryable(t, data.minTime, data.maxTime, lf, cf, nil, matchers...)
			totalFound := 0
			for _, series := range sFound {
				totalFound++
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.seriesHash, series.Labels().Hash())
			}
			require.Equal(t, cfg.metricsPerMetricName, totalFound)
		}
	})

	t.Run("QueryByUniqueLabel and SkipChunks=true", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
		hints := &storage.SelectHints{
			Func: "series",
		}
		sFound := queryWithQueryable(t, data.minTime, data.maxTime, lf, cf, hints, matchers...)
		totalFound := 0
		for _, series := range sFound {
			totalFound++
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
		}
		require.Equal(t, cfg.totalMetricNames, totalFound)
	})

	t.Run("LabelNames", func(t *testing.T) {
		queryable, err := createQueryable(t, lf, cf)
		require.NoError(t, err)
		querier, err := queryable.Querier(data.minTime, data.maxTime)
		require.NoError(t, err)

		t.Run("Without Matchers", func(t *testing.T) {
			lNames, _, err := querier.LabelNames(context.Background(), nil)
			require.NoError(t, err)
			require.NotEmpty(t, lNames)
			expectedLabelNames, err := ir.LabelNames(context.Background())
			require.NoError(t, err)
			require.Equal(t, expectedLabelNames, lNames)
		})

		t.Run("With Matchers", func(t *testing.T) {
			lNames, _, err := querier.LabelNames(context.Background(), nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.NotEmpty(t, lNames)
			expectedLabelNames, err := ir.LabelNames(context.Background(), labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.Equal(t, expectedLabelNames, lNames)
		})
	})

	t.Run("LabelValues", func(t *testing.T) {
		queryable, err := createQueryable(t, lf, cf)
		require.NoError(t, err)
		querier, err := queryable.Querier(data.minTime, data.maxTime)
		require.NoError(t, err)
		t.Run("Without Matchers", func(t *testing.T) {
			lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil)
			require.NoError(t, err)
			expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName)
			require.NoError(t, err)
			require.Equal(t, expectedLabelValues, lValues)
		})

		t.Run("With Matchers", func(t *testing.T) {
			lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.Equal(t, expectedLabelValues, lValues)
		})
	})
}

func queryWithQueryable(t *testing.T, mint, maxt int64, lf, cf *parquet.File, hints *storage.SelectHints, matchers ...*labels.Matcher) []storage.Series {
	ctx := context.Background()
	queryable, err := createQueryable(t, lf, cf)
	require.NoError(t, err)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)
	ss := querier.Select(ctx, true, hints, matchers...)

	found := make([]storage.Series, 0, 100)
	for ss.Next() {
		found = append(found, ss.At())
	}
	return found
}

func createQueryable(t *testing.T, lf *parquet.File, cf *parquet.File) (storage.Queryable, error) {
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	pb, err := NewParquetBlock(lf, cf, d)
	require.NoError(t, err)
	return NewParquetQueryable(pb)
}
