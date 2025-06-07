package storage

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

func TestParquetWriteLocalFromBucketReader(t *testing.T) {
	ctx := context.Background()

	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })
	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal("error creating bucket: ", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := util.DefaultTestConfig()
	data := util.GenerateTestData(t, st, ctx, cfg)

	shardCount, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.MinTime,
		data.MaxTime,
		[]convert.Convertible{st.Head()},
	)
	require.NoError(t, err)
	require.Equal(t, 1, shardCount)

	shardIndex := 0

	//util.ConvertTestDataToParquetBucket(t, ctx, bkt, data, st.Head())

	convertOpts := convert.DefaultConvertOpts

	bucketShard, err := OpenParquetShardFromBucket(ctx, bkt, convertOpts.Name, shardIndex)

	// TODO GET THE NewSplitFileBucketWriterFunc a reader/rowReader for BOTH files in the bucket
	// so that it can write both from the readers; confirm no race condition as the pipewriter sits in between
	// then apply the same to NewSplitFileIOWriterFunc

	bucketRowReader, err := NewBucketRowReader(
		ctx, bkt,
	)

	//localTmpDir := t.TempDir()
	//outDir := filepath.Join(localTmpDir)

	labelsFile := bucketShard.LabelsFile()
	labelsFileSchema, err := schema.FromLabelsFile(labelsFile.File)
	if err != nil {
		t.Fatal("error getting schema from labels file:", err)
	}
	//rowGroupReader, err := NewParquetFileRowGroupReader(labelsFile)
	//if err != nil {
	//	t.Fatal("error creating row group reader:", err)
	//}
	//rowReader, err := rowGroupReader.CurrentRowGroupReader()
	//if err != nil {
	//	t.Fatal("error getting row reader for current row group:", err)
	//}

	rwf := convert.NewSplitFileBucketWriterFunc(bkt)

	//rowWriterFunc := convert.NewSplitFileIOWriterFunc(outDir)
	shardedWriter := convert.NewShardedWrite(rowReader, rwf, labelsFileSchema, bkt, &convertOpts)

	err = shardedWriter.Write(ctx)
	if err != nil {
		t.Fatal("error writing sharded data:", err)
	}
}
