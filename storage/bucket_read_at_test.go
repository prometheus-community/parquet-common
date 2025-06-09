package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// limitedReader is a reader that returns at most n bytes per read
type limitedReader struct {
	r io.Reader
	n int
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if len(p) > r.n {
		p = p[:r.n]
	}
	return r.r.Read(p)
}

// mockBucket implements objstore.Bucket for testing
type mockBucket struct {
	content []byte
}

func (m *mockBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off >= int64(len(m.content)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := off + length
	if end > int64(len(m.content)) {
		end = int64(len(m.content))
	}
	// Create a reader that returns at most 2 bytes per read
	reader := &limitedReader{
		r: bytes.NewReader(m.content[off:end]),
		n: 2,
	}
	return io.NopCloser(reader), nil
}

// Required by objstore.Bucket interface but not used in tests
func (m *mockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) { return nil, nil }

func (m *mockBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return objstore.ObjectAttributes{}, nil
}

func (m *mockBucket) Exists(ctx context.Context, name string) (bool, error) { return false, nil }

func (m *mockBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	return nil
}
func (m *mockBucket) Delete(ctx context.Context, name string) error { return nil }

func (m *mockBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	return nil
}

func (m *mockBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, opts ...objstore.IterOption) error {
	return nil
}
func (m *mockBucket) Close() error { return nil }

func (m *mockBucket) IsAccessDeniedErr(err error) bool { return false }

func (m *mockBucket) IsObjNotFoundErr(err error) bool { return false }

func (m *mockBucket) Name() string { return "test" }

func (m *mockBucket) Provider() objstore.ObjProvider { return objstore.MEMORY }

func (m *mockBucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{}
}

func TestBucketReadAtWithLimitedReader(t *testing.T) {
	// Create test data that's longer than our limited reader's chunk size
	testData := []byte("Hello, this is a test string that is longer than 2 bytes")
	bucket := &mockBucket{content: testData}
	reader := NewBucketReadAt(context.Background(), "test", bucket)

	// Test reading the entire content
	buf := make([]byte, len(testData))
	n, err := reader.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, buf)

	// Test reading a portion of the content
	partialBuf := make([]byte, 10)
	n, err = reader.ReadAt(partialBuf, 7)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, testData[7:17], partialBuf)

	// Test reading past the end
	emptyBuf := make([]byte, 10)
	n, err = reader.ReadAt(emptyBuf, int64(len(testData)+5))
	require.NoError(t, err)
	require.Equal(t, 0, n)
}
