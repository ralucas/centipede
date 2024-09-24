//go:build unit

package transformer_test

import (
	"context"
	"testing"

	"github.com/ralucas/centipede/internal/transformer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTransform(t *testing.T) {
	log, err := zap.NewDevelopment()
	require.NoError(t, err)

	tf := transformer.NewRowTransformer(log)

	testFields := []string{"modified", "contactPoint.fn", "keyword"}
	testMap := map[string]interface{}{
		"modified":        "test-date",
		"contactPoint.fn": "test-contact",
		"keyword":         []interface{}{"a", "b", "c"},
	}

	result, err := tf.Transform(context.TODO(), testMap, testFields)
	assert.NoError(t, err)

	assert.Equal(t, len(result), 3)
	for i := 0; i < len(result); i++ {
		assert.Equal(t, result[i][0], testMap["modified"])
		assert.Equal(t, result[i][1], testMap["contactPoint.fn"])
		assert.Equal(t, result[i][2], testMap["keyword"].([]interface{})[i])
	}
}
