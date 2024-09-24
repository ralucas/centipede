//go:build unit

package etl_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/ralucas/centipede/internal/extractor"
	"github.com/ralucas/centipede/internal/loader"
	"github.com/ralucas/centipede/internal/streamreader"
	"github.com/ralucas/centipede/internal/transformer"
	"github.com/ralucas/centipede/pkg/etl"
	"github.com/ralucas/centipede/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProcess(t *testing.T) {
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
		{name: "success on json array", datasetFile: "dataset_array.json", expect: 2, err: nil, validate: false},
		{name: "success on small json array", datasetFile: "dataset_array_small.json", expect: 3, err: nil, validate: false},
		{name: "fails on invalid json", datasetFile: "dataset_invalid.json", expect: 0, err: errors.New("test"), validate: false},
		{name: "fails on single json object", datasetFile: "dataset_single.json", expect: 0, err: errors.New("test"), validate: false},
		{name: "fails on ndjson", datasetFile: "dataset.ndjson", expect: 0, err: errors.New("test"), validate: false},
		{name: "success on validation", datasetFile: "dataset_array.json", expect: 2, err: nil, validate: true},
		{name: "fails on validation", datasetFile: "invalid_dataset_array.json", expect: 0, err: errors.New("test"), validate: true},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fp, err := f.DatasetFilePath(test.datasetFile)
			require.NoError(t, err)

			file, err := os.Open(fp)
			require.NoError(t, err)

			defer file.Close()

			testFields := []string{"modified", "contactPoint.fn", "keyword"}

			var opts []streamreader.JSONStreamIteratorOption
			if test.validate {
				opts = append(opts, streamreader.WithDatasetValidation())
			}

			processor := etl.NewETLProcessor(
				extractor.NewMapExtractor(log),
				transformer.NewRowTransformer(log),
				loader.NewCSVLoader(log),
				streamreader.NewJSONStreamIterator(file, log, opts...),
				log,
			)

			testFile, err := os.Create(fmt.Sprintf("test_%d.csv", i))
			require.NoError(t, err)
			defer testFile.Close()

			err = processor.Process(context.TODO(), testFile, testFields)
			if test.err == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
