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
	"io"
	"iter"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type Materializer struct {
	b           *storage.ParquetShard
	s           *schema.TSDBSchema
	d           *schema.PrometheusParquetChunksDecoder
	partitioner util.Partitioner

	colIdx      int
	concurrency int

	dataColToIndex []int
}

func NewMaterializer(s *schema.TSDBSchema,
	d *schema.PrometheusParquetChunksDecoder,
	block *storage.ParquetShard,
	concurrency int,
	maxGapPartitioning int,
) (*Materializer, error) {
	colIdx, ok := block.LabelsFile().Schema().Lookup(schema.ColIndexes)
	if !ok {
		return nil, errors.New(fmt.Sprintf("schema index %s not found", schema.ColIndexes))
	}

	dataColToIndex := make([]int, len(block.ChunksFile().Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := block.ChunksFile().Schema().Lookup(schema.DataColumn(i))
		if !ok {
			return nil, errors.New(fmt.Sprintf("schema column %s not found", schema.DataColumn(i)))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		s:              s,
		d:              d,
		b:              block,
		colIdx:         colIdx.ColumnIndex,
		concurrency:    concurrency,
		partitioner:    util.NewGapBasedPartitioner(maxGapPartitioning),
		dataColToIndex: dataColToIndex,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) MaterializeOld(ctx context.Context, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) ([]prom_storage.ChunkSeries, error) {
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	results := make([]prom_storage.ChunkSeries, len(sLbls))
	for i, s := range sLbls {
		sort.Sort(s)
		results[i] = &concreteChunksSeries{
			lbls: s,
		}
	}

	if !skipChunks {
		chks, err := m.materializeChunks(ctx, rgi, mint, maxt, rr)
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to materialize chunks")
		}

		for i, result := range results {
			result.(*concreteChunksSeries).chks = chks[i]
		}

		// If we are not skipping chunks and there is no chunks for the time range queried, lets remove the series
		results = slices.DeleteFunc(results, func(cs prom_storage.ChunkSeries) bool {
			return len(cs.(*concreteChunksSeries).chks) == 0
		})
	}
	return results, err
}

// MaterializeStreaming reconstructs the ChunkSeries with lazy chunk loading.
// Labels are loaded upfront, but chunks are loaded on-demand when iterating.
func (m *Materializer) Materialize(ctx context.Context, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) ([]prom_storage.ChunkSeries, error) {
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	results := make([]prom_storage.ChunkSeries, 0, len(sLbls))

	if skipChunks {
		for _, s := range sLbls {
			sort.Sort(s)

			results = append(results, &concreteChunksSeries{
				lbls: s,
			})
		}
		return results, nil
	}

	i := 0

	for chunks, err := range m.materializeChunksIter(ctx, rgi, mint, maxt, rr) {
		if err != nil {
			return nil, err
		}

		if i >= len(sLbls) {
			return nil, errors.New("more chunks rows than labels rows found, this should not happen")
		}

		s := sLbls[i]
		sort.Sort(s)

		series := &iteratorChunksSeries{
			lbls:   s,
			chunks: chunks,
		}
		if len(chunks) == 0 {
			// If there are no chunks for this series, skip it
			continue
		}
		results = append(results, series)
		i++
	}

	return results, nil
}

func (m *Materializer) MaterializeAllLabelNames() []string {
	r := make([]string, 0, len(m.b.LabelsFile().Schema().Columns()))
	for _, c := range m.b.LabelsFile().Schema().Columns() {
		lbl, ok := schema.ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		r = append(r, lbl)
	}
	return r
}

func (m *Materializer) MaterializeLabelNames(ctx context.Context, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumn(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	seen := make(map[string]struct{})
	colsMap := make(map[string]struct{}, 10)
	for _, colsIdx := range colsIdxs {
		key := util.YoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
			if err != nil {
				return nil, errors.Wrap(err, "failed to decode column index")
			}
			for _, idx := range idxs {
				if _, ok := colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]]; !ok {
					colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]] = struct{}{}
				}
			}
		}
	}
	lbls := make([]string, 0, len(colsMap))
	for col := range colsMap {
		l, ok := schema.ExtractLabelFromColumn(col)
		if !ok {
			return nil, errors.New(fmt.Sprintf("error extracting label name from col %v", col))
		}
		lbls = append(lbls, l)
	}
	return lbls, nil
}

func (m *Materializer) MaterializeLabelValues(ctx context.Context, name string, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	values, err := m.materializeColumn(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	r := make([]string, 0, len(values))
	vMap := make(map[string]struct{}, 10)
	for _, v := range values {
		strValue := util.YoloString(v.ByteArray())
		if _, ok := vMap[strValue]; !ok {
			r = append(r, strValue)
			vMap[strValue] = struct{}{}
		}
	}
	return r, nil
}

func (m *Materializer) MaterializeAllLabelValues(ctx context.Context, name string, rgi int) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	pages, err := m.b.LabelsFile().GetPages(ctx, cc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	p, err := pages.ReadPage()
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to read page")
	}
	defer parquet.Release(p)

	r := make([]string, 0, p.Dictionary().Len())
	for i := 0; i < p.Dictionary().Len(); i++ {
		r = append(r, p.Dictionary().Index(int32(i)).String())
	}
	return r, nil
}

func (m *Materializer) materializeAllLabels(ctx context.Context, rgi int, rr []RowRange) ([]labels.Labels, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumn(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	colsMap := make(map[int]*[]parquet.Value, 10)
	results := make([]labels.Labels, len(colsIdxs))

	for _, colsIdx := range colsIdxs {
		idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to decode column index")
		}
		for _, idx := range idxs {
			colsMap[idx] = &[]parquet.Value{}
		}
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(m.concurrency)

	for cIdx, v := range colsMap {
		errGroup.Go(func() error {
			cc := labelsRg.ColumnChunks()[cIdx]
			values, err := m.materializeColumn(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
			if err != nil {
				return errors.Wrap(err, "failed to materialize labels values")
			}
			*v = values
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	for cIdx, values := range colsMap {
		labelName, ok := schema.ExtractLabelFromColumn(m.b.LabelsFile().Schema().Columns()[cIdx][0])
		if !ok {
			return nil, fmt.Errorf("column %d not found in schema", cIdx)
		}
		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			results[i] = append(results[i], labels.Label{
				Name:  labelName,
				Value: util.YoloString(value.ByteArray()),
			})
		}
	}

	return results, nil
}

func totalRows(rr []RowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.count
	}
	return res
}

func (m *Materializer) materializeChunks(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) ([][]chunks.Meta, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]
	r := make([][]chunks.Meta, totalRows(rr))

	for i := minDataCol; i <= min(maxDataCol, len(m.dataColToIndex)-1); i++ {
		values, err := m.materializeColumn(ctx, m.b.ChunksFile(), rg, rg.ColumnChunks()[m.dataColToIndex[i]], rr)
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

func (m *Materializer) materializeColumn(ctx context.Context, file *storage.ParquetFile, group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) ([]parquet.Value, error) {
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

	pagesToRowsMap := make(map[int][]RowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := RowRange{
			from: oidx.FirstRowIndex(i),
		}
		pageRowRange.count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.count = oidx.FirstRowIndex(i+1) - pageRowRange.from
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
			}
		}
	}

	pageRanges := m.coalescePageRanges(pagesToRowsMap, oidx)

	r := make(map[RowRange][]parquet.Value, len(pageRanges))
	rMutex := &sync.Mutex{}
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)

	for _, p := range pageRanges {
		errGroup.Go(func() error {
			pgs, err := file.GetPages(ctx, cc, p.pages...)
			if err != nil {
				return errors.Wrap(err, "failed to get pages")
			}
			defer func() { _ = pgs.Close() }()
			err = pgs.SeekToRow(p.rows[0].from)
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
						rMutex.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						rMutex.Unlock()
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

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b RowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

// materializeColumnIter returns an iterator that yields values from a parquet column
func (m *Materializer) materializeColumnIter(ctx context.Context, file *storage.ParquetFile, group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) iter.Seq2[parquet.Value, error] {
	return func(yield func(parquet.Value, error) bool) {
		if len(rr) == 0 {
			return
		}

		oidx, err := cc.OffsetIndex()
		if err != nil {
			yield(parquet.Value{}, errors.Wrap(err, "could not get offset index"))
			return
		}

		cidx, err := cc.ColumnIndex()
		if err != nil {
			yield(parquet.Value{}, errors.Wrap(err, "could not get column index"))
			return
		}

		pagesToRowsMap := make(map[int][]RowRange, len(rr))

		for i := 0; i < cidx.NumPages(); i++ {
			pageRowRange := RowRange{
				from: oidx.FirstRowIndex(i),
			}
			pageRowRange.count = group.NumRows()

			if i < oidx.NumPages()-1 {
				pageRowRange.count = oidx.FirstRowIndex(i+1) - pageRowRange.from
			}

			for _, r := range rr {
				if pageRowRange.Overlaps(r) {
					pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
				}
			}
		}

		pageRanges := m.coalescePageRanges(pagesToRowsMap, oidx)

		// TODO: we are losing the concurrency here, is that a trade off or can we do better?
		for _, p := range pageRanges {
			pgs, err := file.GetPages(ctx, cc, p.pages...)
			if err != nil {
				yield(parquet.Value{}, errors.Wrap(err, "failed to get pages"))
				return
			}

			err = pgs.SeekToRow(p.rows[0].from)
			if err != nil {
				pgs.Close()
				yield(parquet.Value{}, errors.Wrap(err, "could not seek to row"))
				return
			}

			vi := new(valuesIterator)
			remainingRr := p.rows
			if len(remainingRr) == 0 {
				pgs.Close()
				continue
			}

			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from
			remainingRr = remainingRr[1:]

			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					pgs.Close()
					yield(parquet.Value{}, errors.Wrap(err, "could not read page"))
					return
				}
				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						if !yield(vi.At(), nil) {
							parquet.Release(page)
							pgs.Close()
							return
						}
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
					pgs.Close()
					yield(parquet.Value{}, vi.Error())
					return
				}
			}
			pgs.Close()
		}
	}
}

type pageEntryRead struct {
	pages []int
	rows  []RowRange
}

// Merge nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func (m *Materializer) coalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []pageEntryRead {
	if len(pagedIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
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
	p parquet.Page

	// TODO: consider using unique.Handle
	cachedSymbols map[int32]parquet.Value
	st            symbolTable

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

var _ prom_storage.ChunkSeries = &concreteChunksSeries{}

type concreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c concreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c concreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(c.chks...)
}

type iteratorChunksSeries struct {
	lbls   labels.Labels
	chunks []chunks.Meta
}

func (i *iteratorChunksSeries) Labels() labels.Labels {
	return i.lbls
}

func (i *iteratorChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(i.chunks...)
}

func (m *Materializer) materializeChunksIter(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) iter.Seq2[[]chunks.Meta, error] {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]

	dataColCount := min(maxDataCol, len(m.dataColToIndex)-1) - minDataCol + 1
	if dataColCount <= 0 {
		return func(yield func([]chunks.Meta, error) bool) {
			for _, r := range rr {
				for i := 0; i < int(r.count); i++ {
					if !yield([]chunks.Meta{}, nil) {
						return
					}
				}
			}
		}
	}
	nexts := make([]func() (parquet.Value, error, bool), dataColCount)
	stoppers := make([]func(), dataColCount)
	// TODO: instead of this, I could define a util Zip(iter.Seq2[k,v]...) iter.Seq2[iter.Seq2[k,v], error] ?
	for i := 0; i < dataColCount; i++ {
		nexts[i], stoppers[i] = iter.Pull2(m.materializeColumnIter(ctx, m.b.ChunksFile(), rg, rg.ColumnChunks()[m.dataColToIndex[minDataCol+i]], rr))
	}
	return func(yield func([]chunks.Meta, error) bool) {
		defer func() {
			for _, stop := range stoppers {
				stop()
			}
		}()
		for {
			rowChunks := make([]chunks.Meta, 0, dataColCount) // TODO: At least one per data column, but can we do better?
			for i := 0; i < dataColCount; i++ {
				value, err, ok := nexts[i]()
				if err != nil {
					yield(nil, errors.Wrap(err, "failed to get next data column value"))
					return
				}
				if !ok {
					// No more values in this column. All columns should have the
					// same number of values, so we can stop here.
					if i != 0 {
						panic("unexpected" + strconv.Itoa(i))
					}
					for j := i + 1; j < dataColCount; j++ {
						_, err, ok := nexts[j]()
						if err != nil {
							panic(errors.Wrap(err, "failed to get next data column value"))
						}
						if ok {
							panic("unexpected value in column " + strconv.Itoa(j))
						}
					}
					return
				}
				chks, err := m.d.Decode(value.ByteArray(), mint, maxt)
				if err != nil {
					yield(nil, errors.Wrap(err, "failed to decode chunks"))
					return
				}
				rowChunks = append(rowChunks, chks...)
			}
			if !yield(rowChunks, nil) {
				return
			}
		}
	}
}
