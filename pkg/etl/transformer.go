package etl

import "context"

type Transformer interface {
	Transform(ctx context.Context, data map[string]interface{}, fields []string) ([][]string, error)
}
