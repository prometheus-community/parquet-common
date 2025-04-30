package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

func TestMaterializeE2E(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)
	seriesHash := make(map[uint64]*struct{})
	totalMetricNames := 1_000
	metricsPerMetricsName := 20
	numberOfLabels := 5
	randomLabels := 3
	numberOfSamples := 250

	builder := labels.NewScratchBuilder(numberOfLabels)

	for i := 0; i < totalMetricNames; i++ {
		for n := 0; n < metricsPerMetricsName; n++ {
			builder.Reset()
			builder.Add(labels.MetricName, fmt.Sprintf("metric_%d", i))
			builder.Add("unique", fmt.Sprintf("unique_%d", n))

			for j := 0; j < numberOfLabels; j++ {
				builder.Add(fmt.Sprintf("label_name_%v", j), fmt.Sprintf("label_value_%v", j))
			}

			firstRandom := rand.Int() % 10
			for k := firstRandom; k < firstRandom+randomLabels; k++ {
				builder.Add(fmt.Sprintf("randon_name_%v", k), fmt.Sprintf("randon_value_%v", k))
			}

			builder.Sort()
			lbls := builder.Labels()
			seriesHash[lbls.Hash()] = &struct{}{}
			for s := 0; s < numberOfSamples; s++ {
				_, err := app.Append(0, lbls, (1 * time.Minute * time.Duration(s)).Milliseconds(), float64(i))
				require.NoError(t, err)
			}
		}
	}

	require.NoError(t, app.Commit())

	h := st.Head()
	colDuration := time.Hour
	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		h.MinTime(),
		h.MaxTime(),
		[]convert.Convertible{h},
		convert.WithName("block"),
		convert.WithColDuration(colDuration), // lets force more than 1 data col
		convert.WithRowGroupSize(500),
		convert.WithPageBufferSize(300), // force create multiples pages
	)

	require.NoError(t, err)
	require.Equal(t, 1, shards)

	labelsFileName := schema.LabelsPfileNameForShard("block", 0)
	chunksFileName := schema.ChunksPfileNameForShard("block", 0)
	lf, cf, err := util.OpenParquetFiles(ctx, bkt, labelsFileName, chunksFileName)
	require.NoError(t, err)

	// Query by unique label (not sorted label)
	eq := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))
	found := query(t, h.MinTime(), h.MaxTime(), lf, cf, eq)
	require.Len(t, found, totalMetricNames)

	for _, series := range found {
		require.Equal(t, series.Labels().Get("unique"), "unique_0")
		require.Contains(t, seriesHash, series.Labels().Hash())
	}

	// Query some random metric name
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("metric_%d", rand.Int()%totalMetricNames)
		eq := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf(name))

		found := query(t, h.MinTime(), h.MaxTime(), lf, cf, eq)

		require.Len(t, found, metricsPerMetricsName, fmt.Sprintf("metric_%d", i))

		for _, series := range found {
			require.Equal(t, series.Labels().Get(labels.MetricName), name)
			require.Contains(t, seriesHash, series.Labels().Hash())

			totalSamples := 0
			ci := series.Iterator(nil)
			for ci.Next() {
				si := ci.At().Chunk.Iterator(nil)
				for si.Next() != chunkenc.ValNone {
					totalSamples++
				}
			}
			require.Equal(t, totalSamples, numberOfSamples)
		}
	}

	// Query block with partial timestamp (make sure we only open the correct cols
	c1 := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf("metric_0"))
	c2 := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))
	found = query(t, h.MinTime(), h.MinTime()+colDuration.Milliseconds()-1, lf, cf, c1, c2)
	require.Len(t, found, 1)
	require.Len(t, found[0].(*concreteChunksSeries).chks, 1)
	found = query(t, h.MinTime(), h.MinTime()+(2*colDuration).Milliseconds()-1, lf, cf, c1, c2)
	require.Len(t, found, 1)
	require.Len(t, found[0].(*concreteChunksSeries).chks, 2)
}

func query(t *testing.T, mint, maxt int64, lf, cf *parquet.File, constraints ...Constraint) []storage.ChunkSeries {
	ctx := context.Background()
	for _, c := range constraints {
		require.NoError(t, c.init(lf.Schema()))
	}

	s, err := schema.FromLabelsFile(lf)
	require.NoError(t, err)
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	m, err := NewMaterializer(s, d, lf, cf)
	require.NoError(t, err)

	found := make([]storage.ChunkSeries, 0, 100)
	for i, group := range lf.RowGroups() {
		rr, err := filter(group, constraints...)
		total := int64(0)
		for _, r := range rr {
			total += r.count
		}
		require.NoError(t, err)
		series, err := m.Materialize(ctx, i, mint, maxt, rr)
		require.NoError(t, err)
		require.Len(t, series, int(total))
		found = append(found, series...)
	}
	return found
}
