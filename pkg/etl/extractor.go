package etl

import "context"

type Extractor interface {
	Extract(ctx context.Context, data map[string]interface{}, fields []string) (map[string]interface{}, error)
}
