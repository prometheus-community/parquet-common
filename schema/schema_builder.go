package schema

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"math"
	"strconv"
	"time"
)

type Builder struct {
	g parquet.Group

	metadata          map[string]string
	dataColDurationMs int64
	minTs, maxTs      int64
}

func NewBuilder(dataColDuration time.Duration) *Builder {
	b := &Builder{
		g:                 make(parquet.Group),
		dataColDurationMs: dataColDuration.Milliseconds(),
		metadata: map[string]string{
			DataColSizeMd: strconv.FormatInt(dataColDuration.Milliseconds(), 10),
		},
		minTs: math.MaxInt64,
	}

	return b
}

func (b *Builder) TrackMinMax(minTs, maxTs int64) {
	b.minTs = min(b.minTs, minTs)
	b.maxTs = max(b.maxTs, maxTs)
}

func (b *Builder) AddLabelNameColumn(lbls []string) {
	for _, lbl := range lbls {
		b.g[LabelToColumn(lbl)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}
}

func (b *Builder) Build() (*TSDBSchema, error) {
	colIdx := 0
	for i := b.minTs; i <= b.maxTs; i += b.dataColDurationMs {
		b.g[DataColumn(colIdx)] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
		colIdx++
	}

	s := parquet.NewSchema("tsdb", b.g)

	dc := make([]int, colIdx)
	for i := range dc {
		lc, ok := s.Lookup(DataColumn(i))
		if !ok {
			return nil, fmt.Errorf("data column %v not found", DataColumn(i))
		}
		dc[i] = lc.ColumnIndex
	}

	return &TSDBSchema{
		Schema:            s,
		Metadata:          b.metadata,
		DataColDurationMs: b.dataColDurationMs,
		DataColsIndexes:   dc,
		MinTs:             b.minTs,
		MaxTs:             b.maxTs,
	}, nil
}

type TSDBSchema struct {
	Schema   *parquet.Schema
	Metadata map[string]string

	DataColsIndexes   []int
	MinTs, MaxTs      int64
	DataColDurationMs int64
}

func (s *TSDBSchema) DataColumIdx(t int64) int {
	colIdx := 0

	for i := s.MinTs + s.DataColDurationMs; i <= t; i += s.DataColDurationMs {
		colIdx++
	}

	return colIdx
}
