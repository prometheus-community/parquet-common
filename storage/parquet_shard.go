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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
)

var DefaultShardOptions = shardOptions{
	optimisticReader: true,
}

type shardOptions struct {
	fileOptions      []parquet.FileOption
	optimisticReader bool
}

type ParquetFile struct {
	*parquet.File
	ReadAtWithContextCloser
	BloomFiltersLoaded bool

	optimisticReader bool
}

type ShardOption func(*shardOptions)

func WithFileOptions(fileOptions ...parquet.FileOption) ShardOption {
	return func(opts *shardOptions) {
		opts.fileOptions = append(opts.fileOptions, fileOptions...)
	}
}

func WithOptimisticReader(optimisticReader bool) ShardOption {
	return func(opts *shardOptions) {
		opts.optimisticReader = optimisticReader
	}
}

func (f *ParquetFile) GetPages(ctx context.Context, cc parquet.ColumnChunk, pagesToRead ...int) (*parquet.FilePages, error) {
	colChunk := cc.(*parquet.FileColumnChunk)
	reader := f.WithContext(ctx)

	if len(pagesToRead) > 0 && f.optimisticReader {
		offset, err := cc.OffsetIndex()
		if err != nil {
			return nil, err
		}
		minOffset := offset.Offset(pagesToRead[0])
		maxOffset := offset.Offset(pagesToRead[len(pagesToRead)-1]) + offset.CompressedPageSize(pagesToRead[len(pagesToRead)-1])
		reader = newOptimisticReaderAt(reader, minOffset, maxOffset)
	}

	pages := colChunk.PagesFrom(reader)
	return pages, nil
}

func Open(ctx context.Context, r ReadAtWithContextCloser, size int64, opts ...ShardOption) (*ParquetFile, error) {
	cfg := DefaultShardOptions

	for _, opt := range opts {
		opt(&cfg)
	}

	c, err := parquet.NewFileConfig(cfg.fileOptions...)
	if err != nil {
		return nil, err
	}

	file, err := parquet.OpenFile(r.WithContext(ctx), size, cfg.fileOptions...)
	if err != nil {
		return nil, err
	}

	return &ParquetFile{
		File:                    file,
		ReadAtWithContextCloser: r,
		BloomFiltersLoaded:      !c.SkipBloomFilters,
		optimisticReader:        cfg.optimisticReader,
	}, nil
}

func OpenFromBucket(ctx context.Context, bkt objstore.BucketReader, name string, opts ...ShardOption) (*ParquetFile, error) {
	attr, err := bkt.Attributes(ctx, name)
	if err != nil {
		return nil, err
	}

	r := NewBucketReadAt(name, bkt)
	return Open(ctx, r, attr.Size, opts...)
}

func OpenFromFile(ctx context.Context, path string, opts ...ShardOption) (*ParquetFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := NewFileReadAt(f)
	pf, err := Open(ctx, r, stat.Size(), opts...)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	// At this point, the file's lifecycle is managed by the ParquetFile
	return pf, nil
}

type ParquetShard interface {
	LabelsFile() *ParquetFile
	ChunksFile() *ParquetFile
	TSDBSchema() (*schema.TSDBSchema, error)
}

type ParquetOpener interface {
	Open(ctx context.Context, path string, opts ...ShardOption) (*ParquetFile, error)
}

type ParquetBucketOpener struct {
	bkt objstore.BucketReader
}

func NewParquetBucketOpener(bkt objstore.BucketReader) *ParquetBucketOpener {
	return &ParquetBucketOpener{
		bkt: bkt,
	}
}

func (o *ParquetBucketOpener) Open(ctx context.Context, name string, opts ...ShardOption) (*ParquetFile, error) {
	return OpenFromBucket(ctx, o.bkt, name, opts...)
}

type ParquetLocalFileOpener struct{}

func NewParquetLocalFileOpener() *ParquetLocalFileOpener {
	return &ParquetLocalFileOpener{}
}

func (o *ParquetLocalFileOpener) Open(ctx context.Context, name string, opts ...ShardOption) (*ParquetFile, error) {
	return OpenFromFile(ctx, name, opts...)
}

type ParquetBucketDownloadLabelsFileOpener struct {
	bucketFileOpener *ParquetBucketOpener
	shard            int
	outDir           string
}

func NewParquetBucketDownloadLabelsFileOpener(
	bucketOpener *ParquetBucketOpener,
	shard int,
	outDir string,
) *ParquetBucketDownloadLabelsFileOpener {
	return &ParquetBucketDownloadLabelsFileOpener{
		bucketFileOpener: bucketOpener,
		shard:            shard,
		outDir:           outDir,
	}
}

func (o *ParquetBucketDownloadLabelsFileOpener) Open(ctx context.Context, name string, opts ...ShardOption) (*ParquetFile, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, o.shard)
	bucketLabelsFile, err := o.bucketFileOpener.Open(ctx, labelsFileName, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "error opening bucket labels parquet file")
	}
	bucketLabelsFileReader := parquet.NewGenericReader[any](bucketLabelsFile)
	labelsFileSchema, err := schema.FromLabelsFile(bucketLabelsFile.File)
	if err != nil {
		return nil, errors.Wrap(err, "error getting schema from bucket labels parquet file")
	}
	labelsProjection, err := labelsFileSchema.LabelsProjection()
	if err != nil {
		return nil, errors.Wrap(err, "error getting schema projection from bucket parquet labels file schema")
	}
	outSchemaProjections := []*schema.TSDBProjection{labelsProjection}

	pipeReaderFileWriter := convert.NewPipeReaderFileWriter(o.outDir)
	shardedBucketToFileWriter := convert.NewShardedWrite(
		bucketLabelsFileReader, labelsFileSchema, outSchemaProjections, pipeReaderFileWriter, &convert.DefaultConvertOpts,
	)
	err = shardedBucketToFileWriter.Write(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error writing bucket labels parquet file to local filesystem")
	}

	filePath := filepath.Join(o.outDir, labelsFileName)
	return NewParquetLocalFileOpener().Open(ctx, filePath, opts...)
}

type ParquetShardSyncOpener struct {
	labelsFile, chunksFile *ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

func NewParquetShardSyncOpener(
	ctx context.Context,
	name string,
	labelsFileOpener ParquetOpener,
	chunksFileOpener ParquetOpener,
	shard int,
	opts ...ShardOption,
) (*ParquetShardSyncOpener, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile *ParquetFile

	errGroup.Go(func() (err error) {
		labelsFile, err = labelsFileOpener.Open(ctx, labelsFileName, opts...)
		return err
	})

	errGroup.Go(func() (err error) {
		chunksFile, err = chunksFileOpener.Open(ctx, chunksFileName, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &ParquetShardSyncOpener{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (s *ParquetShardSyncOpener) LabelsFile() *ParquetFile {
	return s.labelsFile
}

func (s *ParquetShardSyncOpener) ChunksFile() *ParquetFile {
	return s.chunksFile
}

func (s *ParquetShardSyncOpener) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	s.o.Do(func() {
		s.schema, err = schema.FromLabelsFile(s.labelsFile.File)
	})
	return s.schema, err
}

func (s *ParquetShardSyncOpener) Close() error {
	err := &multierror.Error{}
	err = multierror.Append(err, s.labelsFile.Close())
	err = multierror.Append(err, s.chunksFile.Close())
	return err.ErrorOrNil()
}
