package extractor

import (
	"context"
	"errors"
	"strings"

	"go.uber.org/zap"
)

var ErrInvalid = errors.New("failed to validate dataset")

type MapExtractor struct {
	validate bool
	logger   *zap.Logger
}

func NewMapExtractor(log *zap.Logger) *MapExtractor {
	return &MapExtractor{
		logger: log,
	}
}

// Extract takes json bytes and a list of fields to extract to a map.
func (e *MapExtractor) Extract(ctx context.Context, dataset map[string]interface{}, fields []string) (map[string]interface{}, error) {
	extract := make(map[string]interface{})

	for _, field := range fields {
		cur := dataset

		var ok bool
		if !strings.Contains(field, ".") {
			if extract[field], ok = cur[field]; !ok {
				extract[field] = ""
			}
		} else {
			extract[field] = handleNested(strings.Split(field, "."), cur)
		}
	}

	return extract, nil
}

func handleNested(keys []string, obj map[string]interface{}) string {
	cur := obj
	for i := 0; i < len(keys)-1; i++ {
		var val interface{}
		var ok bool

		key := keys[i]
		if val, ok = cur[key]; !ok {
			return ""
		}
		cur = val.(map[string]interface{}) // casting this to a map[string]interface here
	}

	lastKey := keys[len(keys)-1]

	var field string 
	var ok bool

	if field, ok = cur[lastKey].(string); !ok {
		return ""
	}

	return field
}
