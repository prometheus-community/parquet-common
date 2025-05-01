package search

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sort"

	"github.com/efficientgo/core/errors"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

type Materializer struct {
	lf *parquet.File
	cf *parquet.File
	s  *schema.TSDBSchema
	d  *schema.PrometheusParquetChunksDecoder

	colIdx      int
	concurrency int

	dataColToIndex []int
}

func NewMaterializer(s *schema.TSDBSchema, d *schema.PrometheusParquetChunksDecoder, lf, cf *parquet.File) (*Materializer, error) {
	colIdx, ok := lf.Schema().Lookup(schema.ColIndexes)
	if !ok {
		return nil, errors.New(fmt.Sprintf("schema index %s not found", schema.ColIndexes))
	}

	dataColToIndex := make([]int, len(cf.Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := cf.Schema().Lookup(schema.DataColumn(i))
		if !ok {
			return nil, errors.New(fmt.Sprintf("schema column %s not found", schema.DataColumn(i)))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		s:              s,
		d:              d,
		lf:             lf,
		cf:             cf,
		colIdx:         colIdx.ColumnIndex,
		concurrency:    runtime.GOMAXPROCS(0),
		dataColToIndex: dataColToIndex,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) Materialize(ctx context.Context, rgi int, mint, maxt int64, rr []rowRange) ([]storage.ChunkSeries, error) {
	labelsRg := m.lf.RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumn(ctx, labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	colsMap := make(map[int]struct{}, 10)

	results := make([]storage.ChunkSeries, len(colsIdxs))
	for i := 0; i < len(colsIdxs); i++ {
		results[i] = &concreteChunksSeries{}
	}

	for _, colsIdx := range colsIdxs {
		idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to decode column index")
		}
		for _, idx := range idxs {
			colsMap[idx] = struct{}{}
		}
	}

	for cIdx := range colsMap {
		cc := labelsRg.ColumnChunks()[cIdx]
		labelName, ok := schema.ExtractLabelFromColumn(m.lf.Schema().Columns()[cIdx][0])
		if !ok {
			return nil, fmt.Errorf("column %d not found in schema", cIdx)
		}

		values, err := m.materializeColumn(ctx, labelsRg, cc, rr)
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to materialize values")
		}

		for i, value := range values {
			if value.IsNull() {
				continue
			}
			results[i].(*concreteChunksSeries).lbls = append(results[i].(*concreteChunksSeries).lbls, labels.Label{
				Name:  labelName,
				Value: value.String(),
			})
		}
	}

	chks, err := m.materializeChunks(ctx, rgi, mint, maxt, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize chunks")
	}

	for i, result := range results {
		sort.Sort(result.(*concreteChunksSeries).lbls)
		result.(*concreteChunksSeries).chks = chks[i]
	}

	return results, err
}

func (m *Materializer) materializeChunks(ctx context.Context, rgi int, mint, maxt int64, rr []rowRange) ([][]chunks.Meta, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.cf.RowGroups()[rgi]
	totalRows := int64(0)
	for _, r := range rr {
		totalRows += r.count
	}
	r := make([][]chunks.Meta, totalRows)

	for i := minDataCol; i <= maxDataCol; i++ {
		values, err := m.materializeColumn(ctx, rg, rg.ColumnChunks()[m.dataColToIndex[i]], rr)
		if err != nil {
			return r, err
		}

		for vi, value := range values {
			chks, err := m.d.Decode(value.ByteArray(), mint, maxt)
			if err != nil {
				return r, errors.Wrap(err, "failed to decode chunks")
			}
			r[vi] = append(r[vi], chks...)
		}
	}

	return r, nil
}

func (m *Materializer) materializeColumn(ctx context.Context, group parquet.RowGroup, cc parquet.ColumnChunk, rr []rowRange) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

	pagesToRowsMap := make(map[int][]rowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := rowRange{
			from: oidx.FirstRowIndex(i),
		}
		pageRowRange.count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.count = oidx.FirstRowIndex(i+1) - pageRowRange.from
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], r)
			}
		}
	}

	r := make(map[rowRange][]parquet.Value, len(rr))
	for _, v := range rr {
		r[v] = []parquet.Value{}
	}

	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)

	for _, p := range coalescePageRanges(pagesToRowsMap, oidx) {
		errGroup.Go(func() error {
			pgs := m.getPage(ctx, cc)
			defer func() { _ = pgs.Close() }()
			err := pgs.SeekToRow(p.rows[0].from)
			if err != nil {
				return errors.Wrap(err, "could not seek to row")
			}

			vi := new(valuesIterator)
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return errors.Wrap(err, "could not read page")
				}
				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						r[currentRr] = append(r[currentRr], vi.At())
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if vi.Error() != nil {
					return vi.Error()
				}
			}
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize columns")
	}

	values := make([]parquet.Value, 0, 1024)
	for _, v := range rr {
		values = append(values, r[v]...)
	}
	return values, err
}

func (pr *Materializer) getPage(_ context.Context, cc parquet.ColumnChunk) *parquet.FilePages {
	// TODO: pass the context to the reader
	colChunk := cc.(*parquet.FileColumnChunk)
	pages := colChunk.PagesFrom(colChunk.File())
	return pages
}

type pageEntryRead struct {
	pages []int
	rows  []rowRange
}

// Merge nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func coalescePageRanges(pagedIdx map[int][]rowRange, offset parquet.OffsetIndex) []pageEntryRead {
	partitioner := util.NewGapBasedPartitioner(10 * 1024)
	if len(pagedIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := partitioner.Partition(len(idxs), func(i int) (uint64, uint64) {
		return uint64(offset.Offset(idxs[i])), uint64(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pagedIdx[idxs[i]]...)
		}
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}

	return r
}

type valuesIterator struct {
	p             parquet.Page
	st            symbolTable
	cachedSymbols map[int32]parquet.Value

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *valuesIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = nil
	if p.Dictionary() != nil {
		vi.st.Reset(p)
		vi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		vi.vr = p.Values()
		vi.buffer = make([]parquet.Value, 0, 128)
		vi.currentBufferIndex = -1
	}
	vi.current = -1
}

func (vi *valuesIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}

	return true
}

func (vi *valuesIterator) Error() error {
	return vi.err
}

func (vi *valuesIterator) At() parquet.Value {
	if vi.vr == nil {
		dicIndex := vi.st.GetIndex(vi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := vi.cachedSymbols[dicIndex]; !ok {
			vi.cachedSymbols[dicIndex] = vi.st.Get(vi.current).Clone()
		}
		return vi.cachedSymbols[dicIndex]
	}

	return vi.buffer[vi.currentBufferIndex].Clone()
}

var _ storage.ChunkSeries = &concreteChunksSeries{}

type concreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c concreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c concreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return storage.NewListChunkSeriesIterator(c.chks...)
}
