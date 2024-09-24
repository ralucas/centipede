//go:build unit

package extractor_test

import (
	"context"
	"testing"

	"github.com/ralucas/centipede/internal/extractor"
	"github.com/ralucas/centipede/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestExtract(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	f := fixtures.NewTestFixture()
	testData, err := f.DatasetMaps()
	require.NoError(t, err)

	e := extractor.NewMapExtractor(log)

	t.Run("success", func(t *testing.T) {
		testFields := []string{"modified", "contactPoint.fn", "keyword"}

		extract, err := e.Extract(context.TODO(), testData[0], testFields)
		assert.NoError(t, err)

		assert.Equal(t, 3, len(extract))
		assert.Equal(t, "Toni L. Holloway", extract["contactPoint.fn"])
	})

	t.Run("handles nested missing field with empty string", func(t *testing.T) {
		testFields := []string{"modified,publisher.name", "publisher.subOrganizationOf.name", "contactPoint.fn", "keyword"}
		extract, err := e.Extract(context.TODO(), testData[0], testFields)
		assert.NoError(t, err)

		assert.Equal(t, 4, len(extract))
		assert.Empty(t, extract["publisher.subOrganizationOf.name"])
	})

	t.Run("missing field handled with empty string", func(t *testing.T) {
		testFields := []string{"modified", "doesnotexist", "keyword", "contactPoint.doesnotexist"}

		extract, err := e.Extract(context.TODO(), testData[0], testFields)
		assert.NoError(t, err)

		assert.Equal(t, 4, len(extract))
		assert.Empty(t, extract["doesnotexist"])
		assert.Empty(t, extract["contactPoint.doesnotexist"])
	})
}
