package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

// ParquetFileRowGroupReader provides row-oriented access to an open ParquetFile handle
type ParquetFileRowGroupReader struct {
	pqFile             *ParquetFile
	rowGroups          []parquet.RowGroup
	currentRowGroupIdx int
}

// NewParquetFileRowGroupReader creates a new ParquetFileRowGroupReader for an open ParquetFile handle
func NewParquetFileRowGroupReader(
	pqFile *ParquetFile,
) (*ParquetFileRowGroupReader, error) {

	rowGroups := pqFile.RowGroups()
	if len(rowGroups) == 0 {
		pqFile.Close()
		return nil, fmt.Errorf("parquet file has no row groups")
	}

	return &ParquetFileRowGroupReader{
		pqFile:    pqFile,
		rowGroups: rowGroups,
	}, nil
}

func (r *ParquetFileRowGroupReader) CurrentRowGroupReader() (parquet.RowReader, error) {
	if r.currentRowGroupIdx >= len(r.rowGroups) {
		return nil, errors.Wrap(io.EOF, "no more row groups available")
	}
	return parquet.NewRowGroupReader(r.rowGroups[r.currentRowGroupIdx]), nil
}

func (r *ParquetFileRowGroupReader) NextRowGroup() (int, error) {
	if r.currentRowGroupIdx >= len(r.rowGroups) {
		return -1, errors.Wrap(io.EOF, "no more row groups available")
	}

	r.currentRowGroupIdx++
	return r.currentRowGroupIdx, nil
}

// BucketRowReader provides row-oriented access to Parquet files in object storage
type BucketRowReader struct {
	readAtCloser ReadAtWithContextCloser
	file         *ParquetFile
	rowGroup     parquet.RowGroup
}

// NewBucketRowReader creates a new RowReader for Parquet files in object storage
func NewBucketRowReader(
	ctx context.Context,
	bktReader objstore.BucketReader,
	path string,
	opts ...ShardOption,
) (*BucketRowReader, error) {
	bucketReadAtCloser := NewBucketReadAt(path, bktReader)
	//bucketReadAt := bucketReadAtCloser.WithContext(ctx)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to create bucket reader: %w", err)
	//}

	// Open the parquet file
	file, err := OpenFromBucket(ctx, bktReader, path, opts...)
	if err != nil {
		bucketReadAtCloser.Close()
		return nil, fmt.Errorf("failed to open parquet file from bucket: %w", err)
	}

	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		file.Close()
		return nil, fmt.Errorf("parquet file has no row groups")
	}

	return &BucketRowReader{
		readAtCloser: bucketReadAtCloser,
		file:         file,
		rowGroup:     rowGroups[0],
	}, nil
}

// ReadRows reads rows into the provided buffer
func (r *BucketRowReader) ReadRows(rows []parquet.Row) (int, error) {
	reader := parquet.NewRowGroupReader(r.rowGroup)
	return reader.ReadRows(rows)
}

// Schema returns the file schema
func (r *BucketRowReader) Schema() *parquet.Schema {
	return r.file.Schema()
}

// Close releases resources
func (r *BucketRowReader) Close() error {
	return r.readAtCloser.Close()
}
