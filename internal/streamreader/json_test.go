//go:build unit

package streamreader_test

import (
	"errors"
	"os"
	"testing"

	"github.com/ralucas/centipede/internal/streamreader"
	"github.com/ralucas/centipede/pkg/etl"
	"github.com/ralucas/centipede/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStreamIterator(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	f := fixtures.NewTestFixture()

	tests := []struct {
		name        string
		datasetFile string
		expect      int
		validate    bool
		err         error
	}{
		{name: "success on json array", datasetFile: "dataset_array.json", expect: 3, err: nil, validate: false},
		{name: "success on small json array", datasetFile: "dataset_array_small.json", expect: 3, err: nil, validate: false},
		{name: "fails on invalid json", datasetFile: "dataset_invalid.json", expect: 0, err: errors.New("test"), validate: false},
		{name: "fails on single json object", datasetFile: "dataset_single.json", expect: 0, err: errors.New("test"), validate: false},
		{name: "fails on ndjson", datasetFile: "dataset.ndjson", expect: 0, err: errors.New("test"), validate: false},
		{name: "success on validation", datasetFile: "dataset_array.json", expect: 3, err: nil, validate: true},
		{name: "fails on validation", datasetFile: "invalid_dataset_array.json", expect: 0, err: errors.New("test"), validate: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fp, err := f.DatasetFilePath(test.datasetFile)
			require.NoError(t, err)

			file, err := os.Open(fp)
			require.NoError(t, err)

			defer file.Close()

			var opts []streamreader.JSONStreamIteratorOption
			if test.validate {
				opts = append(opts, streamreader.WithDatasetValidation())
			}

			sr := streamreader.NewJSONStreamIterator(file, log, opts...)

			coll := make([]map[string]interface{}, 0)
			for sr.HasNext() {
				obj, err := sr.Next()
				if errors.Is(err, etl.Done) {
					break
				}
				if test.err == nil {
					require.NoError(t, err)
				} else {
					assert.Error(t, err)
					break
				}

				coll = append(coll, obj)
			}

			assert.Equal(t, len(coll), test.expect)
		})
	}
}
