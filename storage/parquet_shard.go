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
	"cmp"
	"context"
	"os"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

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

type ParquetShard struct {
	labelsFile, chunksFile *ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

// OpenParquetShardFromBucket opens the sharded parquet block from the given bucket.
func OpenParquetShardFromBucket(ctx context.Context, bkt objstore.Bucket, name string, shard int, opts ...ShardOption) (*ParquetShard, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile *ParquetFile

	errGroup.Go(func() (err error) {
		labelsFile, err = OpenFromBucket(ctx, bkt, labelsFileName, opts...)
		return err
	})

	errGroup.Go(func() (err error) {
		chunksFile, err = OpenFromBucket(ctx, bkt, chunksFileName, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &ParquetShard{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func NewParquetShard(labelsFile, chunksFile *ParquetFile) *ParquetShard {
	return &ParquetShard{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}
}

func (b *ParquetShard) LabelsFile() *ParquetFile {
	return b.labelsFile
}

func (b *ParquetShard) ChunksFile() *ParquetFile {
	return b.chunksFile
}

func (b *ParquetShard) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	b.o.Do(func() {
		b.schema, err = schema.FromLabelsFile(b.labelsFile.File)
	})
	return b.schema, err
}

func (b *ParquetShard) Close() error {
	err1 := b.labelsFile.Close()
	err2 := b.chunksFile.Close()
	return cmp.Or(err1, err2)
}
