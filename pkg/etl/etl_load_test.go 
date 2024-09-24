//go:build load

package etl_test

import (
	"context"
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
	"go.uber.org/zap/zapcore"
)

func TestProcessWithLargeFile(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	log = log.WithOptions(zap.IncreaseLevel(zapcore.ErrorLevel))

	f := fixtures.NewTestFixture()
	fp, err := f.BuildHugeDatasetFile()
	require.NoError(t, err)

	file, err := os.Open(fp)
	require.NoError(t, err)

	fmt.Println("opened huge file")
	defer file.Close()

	testFields := []string{"modified", "contactPoint.fn", "keyword"}

	processor := etl.NewETLProcessor(
		extractor.NewMapExtractor(log),
		transformer.NewRowTransformer(log),
		loader.NewCSVLoader(log),
		streamreader.NewJSONStreamIterator(file, log),
		log,
	)

	testFile, err := os.Create("test.csv")
	require.NoError(t, err)
	defer testFile.Close()

	fmt.Println("running the etl process...")
	err = processor.Process(context.TODO(), testFile, testFields)
	assert.NoError(t, err)
}
