# Prometheus Parquet Library (WIP)

[![Go Report Card](https://goreportcard.com/badge/github.com/prometheus-community/parquet-common)](https://goreportcard.com/report/github.com/prometheus-community/parquet-common) [![Go Reference](https://pkg.go.dev/badge/github.com/prometheus-community/parquet-common.svg)](https://pkg.go.dev/github.com/prometheus-community/parquet-common) [![Slack](https://img.shields.io/badge/join%20slack-%23prometheus--parquet--dev-brightgreen?style=flat)](https://slack.cncf.io/)

**Status: 🚧 Very early stage – expect breaking changes and rapid iteration**

---

This project aims to provide a **shared Go library** for working with **Parquet**-encoded time series data across Prometheus-related projects such as [Cortex](https://github.com/cortexproject/cortex) and [Thanos](https://github.com/thanos-io/thanos).

## Goal

The core objective is to define a **common Parquet schema** and implement **encoding/decoding logic** that can be reused to:

- Export and query time series data in a Parquet format.
- Enable more efficient and scalable long-term storage in object stores like S3, GCS, and Azure Blob.
- Reduce duplication across projects that are independently experimenting with Parquet-based storage.

## Current Status

This repository is in a **very early phase**. We're still experimenting with the schema design, low-level encoding strategies, and how this can plug into Cortex, Thanos, or other Prometheus-compatible systems.

Expect:

- Rapid changes in structure and API
- Incomplete or unstable features
- Minimal documentation

## Benchmarks Results 

* https://prometheus-community.github.io/parquet-common/dev/bench/

## Usage 

### Converting a TSDB block to Parquet

To convert one or more TSDB block to parquet we can just use the ConvertTSDBBlock as below:

```go
  blockDir := "/path/to/block"
  bkt, _ := filesystem.NewBucket(blockDir)
  tsdbBlock, _ := tsdb.OpenBlock(slog.Default(), blockDir, nil, tsdb.DefaultPostingsDecoderFactory)

  _, err := convert.ConvertTSDBBlock(
    context.Background(),
    bkt,
    tsdbBlock.MinTime(),
    tsdbBlock.MaxTime(),
    []convert.Convertible{tsdbBlock},
    options.WithSortBy(labels.MetricName), // Optional: apply sorting or other options
  )
```
### Querying Parquet File

```go
  blockDir := "/path/to/block"
  ctx := context.Background()

  bkt, _ := filesystem.NewBucket(blockDir)
  bucketOpener := storage.NewParquetBucketOpener(bkt)

  shard, err := storage.NewParquetShardOpener(
	ctx, "shard", bucketOpener, bucketOpener, 0,
  )
  if err != nil {
	t.Fatalf("error opening parquet shard: %v", err)
  }

  decoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
  queryable, _ := NewParquetQueryable(decoder, func(ctx context.Context, mint, maxt int64) ([]storage.ParquetShard, error) {
	return []storage.ParquetShard{shard}, nil
  }) // Implements prometheus/storage.Queryable

  querier, _ := queryable.Querier(mint, maxt)
  set := querier.Select(ctx, true, hints, matchers...) // storage.SeriesSet
```
## Planned Features

- Reusable Go types for time series + metadata
- Parquet schema definitions (with logical type hints for efficiency)
- High-performance encoders/decoders
- Utilities for block indexing and  querying
- Test data generators for benchmarking

## Contributions

Ideas, feedback, and code contributions are welcome — but please note that the design is still in flux. If you're interested in contributing, feel free to open an issue or discussion.