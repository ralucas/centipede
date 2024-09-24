//go:build unit

package custom_test

import (
	"errors"
	"os"
	"testing"

	"github.com/ralucas/centipede/internal/streamreader"
	"github.com/ralucas/centipede/internal/streamreader/custom"
	"github.com/ralucas/centipede/pkg/etl"
	"github.com/ralucas/centipede/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRead(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	f := fixtures.NewTestFixture()

	t.Run("handles invalid json", func(t *testing.T) {
		fp, err := f.DatasetFilePath("dataset_invalid.json")
		require.NoError(t, err)

		file, err := os.Open(fp)
		require.NoError(t, err)

		defer file.Close()

		sr := custom.NewCustomJSONStreamReadIterator(file, log)

		b := make([]byte, 2048)
		n, err := sr.Read(b)

		assert.NoError(t, err)
		assert.Less(t, n, 2048)
	})

	t.Run("handles incomplete json", func(t *testing.T) {
		fp, err := f.DatasetFilePath("dataset_invalid.json")
		require.NoError(t, err)

		file, err := os.Open(fp)
		require.NoError(t, err)

		defer file.Close()

		sr := custom.NewCustomJSONStreamReadIterator(file, log)

		var totalBytes []byte
		var end int
		for {
			b := make([]byte, 1024)
			n, err := sr.Read(b)
			totalBytes = append(totalBytes, b[:n]...)
			end += n
			if errors.Is(err, custom.ErrIncompleteJSON) {
				continue
			} else {
				break
			}
		}

		assert.NoError(t, err)
		assert.Less(t, end, 2048)
	})

	t.Run("handles json array", func(t *testing.T) {
		fp, err := f.DatasetFilePath("dataset_array.json")
		require.NoError(t, err)

		file, err := os.Open(fp)
		require.NoError(t, err)

		sr := custom.NewCustomJSONStreamReadIterator(file, log)
		b := make([]byte, 2048)
		n, err := sr.Read(b)

		assert.NoError(t, err)
		assert.Less(t, n, 2048)
		assert.True(t, sr.IsArray())
	})

	t.Run("handles ndjson", func(t *testing.T) {
		fp, err := f.DatasetFilePath("dataset.ndjson")
		require.NoError(t, err)

		file, err := os.Open(fp)
		require.NoError(t, err)

		sr := custom.NewCustomJSONStreamReadIterator(file, log)
		b := make([]byte, 2048)
		n, err := sr.Read(b)

		assert.NoError(t, err)
		assert.Less(t, n, 2048)
	})
}

func TestIterator(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	f := fixtures.NewTestFixture()

	tests := []struct {
		name        string
		datasetFile string
		expect      int
		err         error
		validate    bool
	}{
		{name: "success on invalid json", datasetFile: "dataset_invalid.json", expect: 2},
		{name: "success on json array", datasetFile: "dataset_array.json", expect: 3},
		{name: "success on small json array", datasetFile: "dataset_array_small.json", expect: 3},
		{name: "success on validation", datasetFile: "dataset_array.json", expect: 3, err: nil, validate: true},
		{name: "fails on validation", datasetFile: "invalid_dataset_array.json", expect: 0, err: streamreader.ErrInvalidDatasetJSON, validate: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fp, err := f.DatasetFilePath(test.datasetFile)
			require.NoError(t, err)

			file, err := os.Open(fp)
			require.NoError(t, err)

			defer file.Close()

			var opts []custom.JSONStreamReadIteratorOption
			if test.validate {
				opts = append(opts, custom.WithDatasetValidation())
			}

			sr := custom.NewCustomJSONStreamReadIterator(file, log, opts...)

			coll := make([]map[string]interface{}, 0)
			for sr.HasNext() {
				obj, err := sr.Next()
				if errors.Is(err, etl.Done) {
					break
				}
				if test.err == nil {
					assert.NoError(t, err)
				} else {
					assert.ErrorIs(t, err, streamreader.ErrInvalidDatasetJSON)
					break
				}

				coll = append(coll, obj)
			}

			assert.Equal(t, test.expect, len(coll))
		})
	}
}
