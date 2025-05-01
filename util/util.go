package util

import (
	"context"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
)

func YoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

func CloneRows(rows []parquet.Row) []parquet.Row {
	rr := make([]parquet.Row, len(rows))
	for i, row := range rows {
		rr[i] = row.Clone()
	}
	return rr
}

func OpenParquetFiles(ctx context.Context, bkt objstore.Bucket, labelsFileName, chunksFileName string) (*parquet.File, *parquet.File, error) {
	labelsAttr, err := bkt.Attributes(ctx, labelsFileName)
	if err != nil {
		return nil, nil, err
	}
	labelsFile, err := parquet.OpenFile(NewBucketReadAt(ctx, labelsFileName, bkt), labelsAttr.Size)
	if err != nil {
		return nil, nil, err
	}

	chunksAttr, err := bkt.Attributes(ctx, chunksFileName)
	if err != nil {
		return nil, nil, err
	}

	chunksFile, err := parquet.OpenFile(NewBucketReadAt(ctx, chunksFileName, bkt), chunksAttr.Size)
	if err != nil {
		return nil, nil, err
	}
	return labelsFile, chunksFile, nil
}

// Copied from thanos repository:
// https://github.com/thanos-io/thanos/blob/2a5a856e34adb2653dda700c4d87637236afb2dd/pkg/store/bucket.go#L3466

type Part struct {
	Start uint64
	End   uint64

	ElemRng [2]int
}

type Partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []Part
}

type gapBasedPartitioner struct {
	maxGapSize uint64
}

func NewGapBasedPartitioner(maxGapSize uint64) Partitioner {
	return gapBasedPartitioner{
		maxGapSize: maxGapSize,
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End+g.maxGapSize < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}
