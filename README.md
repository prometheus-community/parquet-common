# Prometheus Parquet Common Library

[![Go Report Card](https://goreportcard.com/badge/github.com/prometheus-community/parquet-common)](https://goreportcard.com/report/github.com/prometheus-community/parquet-common) [![Go Reference](https://pkg.go.dev/badge/github.com/prometheus-community/parquet-common.svg)](https://pkg.go.dev/github.com/prometheus-community/parquet-common) [![Slack](https://img.shields.io/badge/join%20slack-%23prometheus--parquet--dev-brightgreen?style=flat)](https://slack.cncf.io/)

**Status: 🚧 Early development – expect breaking changes**

A shared Go library for working with Parquet-encoded Prometheus time series data, designed for use across projects like [Cortex](https://github.com/cortexproject/cortex) and [Thanos](https://github.com/thanos-io/thanos).

## Overview

This library provides a standardized approach to storing and querying Prometheus time series data in Parquet format, enabling:

- **Efficient long-term storage** in object stores (S3, GCS, Azure Blob)
- **Columnar compression** for better storage efficiency
- **Fast analytical queries** on historical time series data
- **Shared schema** to reduce duplication across Prometheus ecosystem projects

## Key Features

- **TSDB Block Conversion**: Convert Prometheus TSDB blocks to Parquet format
- **Prometheus-Compatible Querying**: Implements `storage.Queryable` interface
- **Flexible Schema**: Configurable column layout and compression options
- **Performance Optimized**: Built-in bloom filters, sorting, and chunking strategies
- **Cloud Storage Ready**: Works with any `objstore.Bucket` implementation

## Architecture

The library is organized into several key packages:

- **`convert`**: TSDB block to Parquet conversion utilities
- **`schema`**: Parquet schema definitions and encoding/decoding logic
- **`queryable`**: Prometheus-compatible query interface implementation
- **`search`**: Query optimization and constraint handling

## Quick Start

### Installation

```bash
go get github.com/prometheus-community/parquet-common
```

### Converting TSDB Blocks to Parquet

Convert Prometheus TSDB blocks to Parquet format for long-term storage:

```go
package main

import (
    "context"
    "log/slog"
    
    "github.com/prometheus/prometheus/tsdb"
    "github.com/thanos-io/objstore/providers/filesystem"
    
    "github.com/prometheus-community/parquet-common/convert"
)

func main() {
    ctx := context.Background()
    blockDir := "/path/to/tsdb/block"
    
    // Set up storage bucket
    bkt, err := filesystem.NewBucket(blockDir)
    if err != nil {
        panic(err)
    }
    defer bkt.Close()
    
    // Open TSDB block
    tsdbBlock, err := tsdb.OpenBlock(slog.Default(), blockDir, nil, tsdb.DefaultPostingsDecoderFactory)
    if err != nil {
        panic(err)
    }
    defer tsdbBlock.Close()
    
    // Convert to Parquet
    _, err = convert.ConvertTSDBBlock(
        ctx,
        bkt,
        tsdbBlock.MinTime(),
        tsdbBlock.MaxTime(),
        []convert.Convertible{tsdbBlock},
        convert.WithSortBy("__name__"), // Sort by metric name for better compression
    )
    if err != nil {
        panic(err)
    }
}
```

### Querying Parquet Data

Query Parquet files using the Prometheus-compatible interface:

```go
package main

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/queryable"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

func main() {
	ctx := context.Background()

	// Set up storage
	bkt, _ := filesystem.NewBucket("/path/to/parquet/data")
	bucketOpener := storage.NewParquetBucketOpener(bkt)

	// Open parquet shard
	shard, err := storage.NewParquetShardOpener(
		ctx, "shard", bucketOpener, bucketOpener, 0,
	)
	if err != nil {
		panic(err)
	}

	// Create queryable interface
	decoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	queryable, _ := queryable.NewParquetQueryable(decoder, func(ctx context.Context, mint, maxt int64) ([]storage.ParquetShard, error) {
		return []storage.ParquetShard{shard}, nil
	})

	// Query data
	querier, err := queryable.Querier(0, math.MaxInt64) // Query all time
	if err != nil {
		panic(err)
	}
	defer querier.Close()

	// Select series matching criteria
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_usage"),
	}

	seriesSet := querier.Select(ctx, false, nil, matchers...)

	// Iterate through results
	for seriesSet.Next() {
		series := seriesSet.At()
		fmt.Printf("Series: %s\n", series.Labels().String())

		iterator := series.Iterator(nil)
		for iterator.Next() == chunkenc.ValFloat {
			ts, val := iterator.At()
			fmt.Printf("  %d: %f\n", ts, val)
		}
	}
}

```

## Configuration Options

### Conversion Options

Customize the conversion process with various options:

```go
_, err := convert.ConvertTSDBBlock(
    ctx, bkt, minTime, maxTime, blocks,
    convert.WithName("custom-block-name"),
    convert.WithRowGroupSize(500000),                    // Rows per row group
    convert.WithColumnDuration(4*time.Hour),             // Time span per column
    convert.WithSortBy("__name__", "instance"),          // Sort order for better compression
    convert.WithBloomFilterLabels("__name__", "job"),    // Labels to create bloom filters for
    convert.WithReadConcurrency(8),                          // Parallel processing
    convert.WithCompression(schema.CompressionZstd),     // Compression algorithm
)
```

### Schema Configuration

The library uses a columnar schema optimized for time series data:

- **Label columns**: Prefixed with `l_` (e.g., `l___name__`, `l_instance`)
- **Data columns**: Time-partitioned chunks prefixed with `s_data_`
- **Index column**: `s_col_indexes` for efficient chunk lookup
- **Metadata**: Min/max timestamps and column duration information
## Performance

The library is designed for high performance with several optimizations:

- **Columnar storage** with efficient compression (Zstd, Snappy)
- **Bloom filters** on frequently queried labels
- **Time-based partitioning** for faster range queries
- **Parallel processing** during conversion and querying

### Benchmarks

Performance benchmarks are available at: https://prometheus-community.github.io/parquet-common/dev/bench/

Run benchmarks locally:

```bash
make bench-select  # Query performance benchmarks
```

## Development

### Prerequisites

- Go 1.23.4 or later
- Make

### Building and Testing

```bash
# Run tests
make test-short

# Run all tests with coverage
make all-tests-with-coverage
```

### Project Structure

```
├── convert/          # TSDB to Parquet conversion
├── queryable/        # Prometheus-compatible query interface
├── schema/           # Parquet schema and encoding/decoding
├── search/           # Query optimization and constraints
├── storage/          # Parquet file management
└── util/             # Common utilities
```

## Current Status & Roadmap

**Current Status**: Early development with core functionality implemented

**Completed**:
- ✅ TSDB block to Parquet conversion
- ✅ Prometheus-compatible querying interface
- ✅ Configurable schema and compression
- ✅ Performance optimizations (bloom filters, sorting, opmistic reader)

**In Progress**:
- 🔄 API stabilization
- 🔄 Enhanced query performance
- 🔄 Documentation improvements

**Planned**:
- 📋 Streaming query capabilities

## Contributing

We welcome contributions! This project is in active development, so please:

1. **Check existing issues** before starting work
2. **Open an issue** to discuss significant changes
3. **Follow Go best practices** and include tests
4. **Update documentation** for user-facing changes

### Getting Help

- **GitHub Issues**: Bug reports and feature requests
- **Slack**: Join [#prometheus-parquet-dev](https://slack.cncf.io/) for discussions
- **Documentation**: Check the [Go Reference](https://pkg.go.dev/github.com/prometheus-community/parquet-common)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.