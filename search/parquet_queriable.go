package search

import (
	"context"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

type parquetQueryable struct {
	blocks []*ParquetBlock
}

func NewParquetQueryable(blocks ...*ParquetBlock) (storage.Queryable, error) {
	return &parquetQueryable{
		blocks: blocks,
	}, nil
}

func (p parquetQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &parquetQuerier{
		mint:   mint,
		maxt:   maxt,
		blocks: p.blocks,
	}, nil
}

type parquetQuerier struct {
	mint, maxt int64

	blocks []*ParquetBlock
}

func (p parquetQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameValues := [][]string{}

	for _, b := range p.blocks {
		r, err := b.labelValues(ctx, name, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameValues = append(resNameValues, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameValues...), nil, nil
}

func (p parquetQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameSets := [][]string{}

	for _, b := range p.blocks {
		r, err := b.labelNames(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameSets = append(resNameSets, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameSets...), nil, nil
}

func (p parquetQuerier) Close() error {
	return nil
}

func (p parquetQuerier) Select(ctx context.Context, sorted bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	seriesSet := make([]storage.ChunkSeriesSet, len(p.blocks))

	minT, maxT := p.mint, p.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	for i, block := range p.blocks {
		ss, err := block.query(ctx, sorted, minT, maxT, matchers)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		seriesSet[i] = ss
	}
	return storage.NewSeriesSetFromChunkSeriesSet(
		convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, storage.NewConcatenatingChunkSeriesMerger()),
	)
}

type ParquetBlock struct {
	lf, cf *parquet.File
	m      *Materializer
}

func NewParquetBlock(lf, cf *parquet.File, d *schema.PrometheusParquetChunksDecoder) (*ParquetBlock, error) {
	s, err := schema.FromLabelsFile(lf)
	if err != nil {
		return nil, err
	}
	m, err := NewMaterializer(s, d, lf, cf)
	if err != nil {
		return nil, err
	}

	return &ParquetBlock{
		lf: lf,
		cf: cf,
		m:  m,
	}, nil
}

func (b ParquetBlock) query(ctx context.Context, sorted bool, mint, maxt int64, matchers []*labels.Matcher) (storage.ChunkSeriesSet, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf.Schema(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([]storage.ChunkSeries, 0, 1024)
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.Materialize(ctx, i, mint, maxt, rr)
		if err != nil {
			return nil, err
		}
		results = append(results, series...)
	}

	if sorted {
		sort.Sort(byLabels(results))
	}
	return convert.NewChunksSeriesSet(results), nil
}

func (b ParquetBlock) labelNames(ctx context.Context, matchers []*labels.Matcher) ([][]string, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf.Schema(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.lf.RowGroups()))
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelNames(ctx, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

func (b ParquetBlock) labelValues(ctx context.Context, name string, matchers []*labels.Matcher) ([][]string, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf.Schema(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.lf.RowGroups()))
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelValues(ctx, name, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

type byLabels []storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }
