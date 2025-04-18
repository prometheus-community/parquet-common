// Copyright 2025 The Prometheus Authors
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
	"io"
	"slices"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type tsdbRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	rowBuilder *parquet.RowBuilder
	schema     *schema.TSDBSchema

	encoder *schema.PrometheusParquetChunksEncoder
}

func newTsdbRowReader(ctx context.Context, mint, maxt, colDuration int64, blks []Convertible, sortedLabels ...string) (*tsdbRowReader, error) {
	var (
		seriesSets = make([]storage.ChunkSeriesSet, 0, len(blks))
		closers    = make([]io.Closer, 0, len(blks))
	)

	b := schema.NewBuilder(mint, maxt, colDuration)

	compareFunc := func(a, b labels.Labels) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return 0
	}

	for _, blk := range blks {
		indexr, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("unable to get index reader from block: %s", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %s", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %s", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %s", err)
		}

		postings := sortedPostings(ctx, indexr, compareFunc, sortedLabels...)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)

		b.AddLabelNameColumn(lblns...)
	}

	cseriesSet := NewMergeChunkSeriesSet(seriesSets, compareFunc, storage.NewConcatenatingChunkSeriesMerger())

	s, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("unable to build index reader from block: %s", err)
	}

	return &tsdbRowReader{
		ctx:       ctx,
		seriesSet: cseriesSet,
		closers:   closers,
		schema:    s,

		rowBuilder: parquet.NewRowBuilder(s.Schema),
		encoder:    schema.NewPrometheusParquetChunksEncoder(s),
	}, nil
}

func (rr *tsdbRowReader) Close() error {
	err := &multierror.Error{}
	for i := range rr.closers {
		err = multierror.Append(err, rr.closers[i].Close())
	}
	return err.ErrorOrNil()
}

func (rr *tsdbRowReader) Schema() *parquet.Schema {
	return rr.schema.Schema
}

func sortedPostings(ctx context.Context, indexr tsdb.IndexReader, compare func(a, b labels.Labels) int, sortedLabels ...string) index.Postings {
	p := tsdb.AllSortedPostings(ctx, indexr)

	if len(sortedLabels) == 0 {
		return p
	}

	type s struct {
		ref    storage.SeriesRef
		labels labels.Labels
	}
	series := make([]s, 0, 128)

	lb := labels.NewScratchBuilder(10)
	for p.Next() {
		lb.Reset()
		err := indexr.Series(p.At(), &lb, nil)
		if err != nil {
			return index.ErrPostings(fmt.Errorf("expand series: %w", err))
		}

		series = append(series, s{labels: lb.Labels().MatchLabels(true, sortedLabels...), ref: p.At()})
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(fmt.Errorf("expand postings: %w", err))
	}

	slices.SortFunc(series, func(a, b s) int { return compare(a.labels, b.labels) })

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

func (rr *tsdbRowReader) ReadRows(buf []parquet.Row) (int, error) {
	select {
	case <-rr.ctx.Done():
		return 0, rr.ctx.Err()
	default:
	}

	var it chunks.Iterator

	i := 0
	for i < len(buf) && rr.seriesSet.Next() {
		rr.rowBuilder.Reset()
		s := rr.seriesSet.At()
		it = s.Iterator(it)

		chkBytes, err := rr.encoder.Encode(it)
		if err != nil {
			return i, fmt.Errorf("unable to collect chunks: %s", err)
		}

		// skip series that have no chunks in the requested time
		if allChunksEmpty(chkBytes) {
			continue
		}

		s.Labels().Range(func(l labels.Label) {
			colName := schema.LabelToColumn(l.Name)
			lc, _ := rr.schema.Schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
		})

		for idx, chk := range chkBytes {
			if len(chk) == 0 {
				continue
			}
			rr.rowBuilder.Add(rr.schema.DataColsIndexes[idx], parquet.ValueOf(chk))
		}
		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	if i < len(buf) {
		return i, io.EOF
	}
	return i, rr.seriesSet.Err()
}

func allChunksEmpty(chkBytes [][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}
