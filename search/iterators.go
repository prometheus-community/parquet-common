package search

import (
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
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

func (c concreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

type rowValue struct {
	row int64
	val parquet.Value
}

// rowRangesValuesIterator yields individual parquet Values from specified row ranges in its FilePages
type rowRangesValuesIterator struct {
	pgs          *parquet.FilePages
	pageIterator *pageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	buffer             []rowValue
	currentBufferIndex int
	err                error
}

func (ri *rowRangesValuesIterator) At() (int64, parquet.Value) {
	return ri.buffer[ri.currentBufferIndex].row, ri.buffer[ri.currentBufferIndex].val
}

func (ri *rowRangesValuesIterator) Next() bool {
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
			ri.err = errors.Wrap(err, "could not read page")
			return false
		}
		ri.pageIterator.Reset(page)
		// Reset page values buffer
		ri.currentBufferIndex = 0
		ri.buffer = []rowValue{}

		for ri.pageIterator.Next() {
			if ri.currentRow == ri.next {
				found = true
				ri.buffer = append(ri.buffer, rowValue{ri.currentRow, ri.pageIterator.At()})

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
			ri.err = errors.Wrap(ri.pageIterator.Err(), "could not read page values")
			return false
		}
	}
	return found
}

func (ri *rowRangesValuesIterator) Err() error {
	return ri.err
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
		pi.buffer = make([]parquet.Value, 0, 128)
		pi.currentBufferIndex = -1
	}
	pi.current = -1
}

func (pi *pageValueIterator) Err() error {
	return pi.err
}
