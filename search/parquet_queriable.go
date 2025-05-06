package search

import (
	"context"

	"github.com/prometheus-community/parquet-common/convert"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/schema"
)

type parquetQueryable struct {
	lf, cf *parquet.File
	m      *Materializer
}

func newParquetQueryable(lf, cf *parquet.File, d *schema.PrometheusParquetChunksDecoder) (storage.Queryable, error) {
	s, err := schema.FromLabelsFile(lf)
	if err != nil {
		return nil, err
	}
	m, err := NewMaterializer(s, d, lf, cf)
	if err != nil {
		return nil, err
	}
	return &parquetQueryable{
		lf: lf,
		cf: cf,
		m:  m,
	}, nil
}

func (p parquetQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &parquetQuerier{
		mint: mint,
		maxt: maxt,
		lf:   p.lf,
		cf:   p.cf,
		m:    p.m,
	}, nil
}

type parquetQuerier struct {
	mint, maxt int64

	lf, cf *parquet.File
	m      *Materializer
}

func (p parquetQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO implement me
	panic("implement me")
}

func (p parquetQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO implement me
	panic("implement me")
}

func (p parquetQuerier) Close() error {
	return nil
}

func (p parquetQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	err = Initialize(p.lf.Schema(), cs...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	seriesSets := make([]storage.ChunkSeriesSet, len(p.lf.RowGroups()))
	for i, group := range p.lf.RowGroups() {
		rr, err := Filter(group, cs...)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		series, err := p.m.Materialize(ctx, i, p.mint, p.maxt, rr)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		seriesSets[i] = convert.NewChunksSeriesSet(series)
	}

	return storage.NewSeriesSetFromChunkSeriesSet(
		convert.NewMergeChunkSeriesSet(seriesSets, labels.Compare, storage.NewConcatenatingChunkSeriesMerger()),
	)
}
