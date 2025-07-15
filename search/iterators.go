package search

import (
	"context"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

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

type chunkIterableSet interface {
	Next() bool
	At() chunks.Iterator
	Err() error
	Close()
}

// multiColumnChunksDecodingIterator yields a prometheus chunks.Iterator from multiple parquet Columns.
// The column iterators are called in order for each column and zipped together,
// yielding a single iterator that in turn can yield all chunks for the same row.
//
// The "column iterators" are pagesRowValueIterator which are expected to be:
// 1. in order of the columns in the parquet file
// 2. initialized with the same set of pages and row ranges
// An error is returned if the iterators have different lengths.
type multiColumnChunksDecodingIterator struct {
	mint int64
	maxt int64

	columnValueIterators []*columnValueIterator
	d                    *schema.PrometheusParquetChunksDecoder

	// current is a chunk-decoding iterator for the materialized parquet Values
	// combined by calling and zipping all column iterators in order
	current *valueDecodingChunkIterator
	err     error
}

func (c *multiColumnChunksDecodingIterator) At() chunks.Iterator {
	return c.current
}

func (c *multiColumnChunksDecodingIterator) Next() bool {
	if c.err != nil || len(c.columnValueIterators) == 0 {
		return false
	}

	multiColumnValues := make([]parquet.Value, 0, len(c.columnValueIterators))
	for _, columnValueIter := range c.columnValueIterators {
		if !columnValueIter.Next() {
			c.err = columnValueIter.Err()
			if c.err != nil {
				return false
			}
			continue
		}
		at := columnValueIter.At()
		multiColumnValues = append(multiColumnValues, at)
	}
	if len(multiColumnValues) == 0 {
		return false
	}

	c.current = &valueDecodingChunkIterator{
		mint:   c.mint,
		maxt:   c.maxt,
		values: multiColumnValues,
		d:      c.d,
	}
	return true
}

func (c *multiColumnChunksDecodingIterator) Err() error {
	return c.err
}

func (c *multiColumnChunksDecodingIterator) Close() {
}

// valueDecodingChunkIterator decodes and yields chunks from a parquet Values slice.
type valueDecodingChunkIterator struct {
	// TODO: why do we need these for decoding? can't we discard out of range chunks in advance?
	mint   int64
	maxt   int64
	values []parquet.Value
	d      *schema.PrometheusParquetChunksDecoder

	decoded []chunks.Meta
	current chunks.Meta
	err     error
}

func (c *valueDecodingChunkIterator) At() chunks.Meta {
	return c.current
}

func (c *valueDecodingChunkIterator) Next() bool {
	if c.err != nil {
		return false
	}
	if len(c.values) == 0 && len(c.decoded) == 0 {
		return false
	}
	if len(c.decoded) > 0 {
		c.current = c.decoded[0]
		c.decoded = c.decoded[1:]
		return true
	}
	value := c.values[0]
	c.values = c.values[1:]

	// TODO: we can do better at pooling here.
	c.decoded, c.err = c.d.Decode(value.ByteArray(), c.mint, c.maxt)
	return c.Next()
}

func (c *valueDecodingChunkIterator) Err() error {
	return c.err
}

func (c concreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

type columnValueIterator struct {
	currentIteratorIndex int
	rowRangesIterators   []*rowRangesValueIterator

	current parquet.Value
	err     error
}

func (ci *columnValueIterator) At() parquet.Value {
	return ci.current
}

func (ci *columnValueIterator) Next() bool {
	if ci.err != nil {
		return false
	}

	found := false
	for !found {
		if ci.currentIteratorIndex >= len(ci.rowRangesIterators) {
			return false
		}

		currentIterator := ci.rowRangesIterators[ci.currentIteratorIndex]
		hasNext := currentIterator.Next()

		if !hasNext {
			if err := currentIterator.Err(); err != nil {
				ci.err = err
				_ = currentIterator.Close()
				return false
			}
			// Iterator exhausted without error; close and move on to the next one
			_ = currentIterator.Close()
			ci.currentIteratorIndex++
			continue
		}
		ci.current = currentIterator.At()
		found = true
	}
	return found
}

func (ci *columnValueIterator) Err() error {
	return ci.err
}

// rowRangesValueIterator yields individual parquet Values from specified row ranges in its FilePages
type rowRangesValueIterator struct {
	pgs          *parquet.FilePages
	pageIterator *pageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func newRowRangesValueIterator(
	ctx context.Context,
	file *storage.ParquetFile,
	cc parquet.ColumnChunk,
	pageRange pageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
) (*rowRangesValueIterator, error) {
	minOffset := uint64(pageRange.off)
	maxOffset := uint64(pageRange.off + pageRange.csz)

	// if dictOff == 0, it means that the collum is not dictionary encoded
	if dictOff > 0 && int(minOffset-(dictOff+dictSz)) < file.Cfg.PagePartitioningMaxGapSize {
		minOffset = dictOff
	}

	pgs, err := file.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		if pgs != nil {
			_ = pgs.Close()
		}
		return nil, errors.Wrap(err, "failed to get pages")
	}

	err = pgs.SeekToRow(pageRange.rows[0].from)
	if err != nil {
		_ = pgs.Close()
		return nil, errors.Wrap(err, "failed to seek to row")
	}

	remainingRr := pageRange.rows

	currentRr := remainingRr[0]
	next := currentRr.from
	remaining := currentRr.count
	currentRow := currentRr.from

	remainingRr = remainingRr[1:]
	return &rowRangesValueIterator{
		pgs:          pgs,
		pageIterator: new(pageValueIterator),

		remainingRr: remainingRr,
		currentRr:   currentRr,
		next:        next,
		remaining:   remaining,
		currentRow:  currentRow,
	}, nil
}

func (ri *rowRangesValueIterator) At() parquet.Value {
	return ri.buffer[ri.currentBufferIndex]
}

func (ri *rowRangesValueIterator) Next() bool {
	if ri.err != nil {
		return false
	}

	if len(ri.buffer) > 0 && ri.currentBufferIndex < len(ri.buffer)-1 {
		// Still have buffered values from previous page reads to yield
		ri.currentBufferIndex++
		return true
	}

	if len(ri.remainingRr) == 0 && ri.remaining == 0 {
		// Done; all rows of all pages have been read
		// and all buffered values from the row ranges have been yielded
		return false
	}

	// Read pages until we find values for the next row range.
	found := false
	for !found {
		// Prepare inner iterator
		page, err := ri.pgs.ReadPage()
		if err != nil {
			ri.err = errors.Wrap(err, "failed to read page")
			return false
		}
		ri.pageIterator.Reset(page)
		// Reset page values buffer
		ri.currentBufferIndex = 0
		ri.buffer = ri.buffer[:0]

		for ri.pageIterator.Next() {
			if ri.currentRow == ri.next {
				found = true
				ri.buffer = append(ri.buffer, ri.pageIterator.At())

				ri.remaining--
				if ri.remaining > 0 {
					ri.next = ri.next + 1
				} else if len(ri.remainingRr) > 0 {
					ri.currentRr = ri.remainingRr[0]
					ri.next = ri.currentRr.from
					ri.remaining = ri.currentRr.count
					ri.remainingRr = ri.remainingRr[1:]
				}
			}
			ri.currentRow++
		}
		parquet.Release(page)
		if ri.pageIterator.Err() != nil {
			ri.err = errors.Wrap(ri.pageIterator.Err(), "failed to read page values")
			return false
		}
	}
	return found
}

func (ri *rowRangesValueIterator) Err() error {
	return ri.err
}

func (ri *rowRangesValueIterator) Close() error {
	return ri.pgs.Close()
}

// pageValueIterator yields individual parquet Values from its Page.
type pageValueIterator struct {
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

func (pi *pageValueIterator) At() parquet.Value {
	if pi.vr == nil {
		dicIndex := pi.st.GetIndex(pi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := pi.cachedSymbols[dicIndex]; !ok {
			pi.cachedSymbols[dicIndex] = pi.st.Get(pi.current).Clone()
		}
		return pi.cachedSymbols[dicIndex]
	}

	return pi.buffer[pi.currentBufferIndex].Clone()
}

func (pi *pageValueIterator) Next() bool {
	if pi.err != nil {
		return false
	}

	pi.current++
	if pi.current >= int(pi.p.NumRows()) {
		return false
	}

	pi.currentBufferIndex++

	if pi.currentBufferIndex == len(pi.buffer) {
		n, err := pi.vr.ReadValues(pi.buffer[:cap(pi.buffer)])
		if err != nil && err != io.EOF {
			pi.err = err
		}
		pi.buffer = pi.buffer[:n]
		pi.currentBufferIndex = 0
	}

	return true
}

func (pi *pageValueIterator) Reset(p parquet.Page) {
	pi.p = p
	pi.vr = nil
	if p.Dictionary() != nil {
		pi.st.Reset(p)
		pi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		pi.vr = p.Values()
		if pi.buffer != nil {
			pi.buffer = pi.buffer[:0]
		} else {
			pi.buffer = make([]parquet.Value, 0, 128)
		}
		pi.currentBufferIndex = -1
	}
	pi.current = -1
}

func (pi *pageValueIterator) Err() error {
	return pi.err
}
