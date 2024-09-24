//go:build unit

package loader_test

import (
	"context"
	"os"
	"testing"

	"github.com/ralucas/centipede/internal/loader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoad(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	l := loader.NewCSVLoader(log)

	testFields := []string{"modified", "contactPoint.fn", "keyword"}
	
	testData := [][]string{
		testFields,
		{"a", "b", "c"},
		{"a", "b", "d"},
	}
	
	f, err := os.Create("test.csv")
	require.NoError(t, err)

	defer f.Close()
	
	err = l.Load(context.TODO(), testData, f)
	
	assert.NoError(t, err)
}