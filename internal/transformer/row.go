package transformer

import (
	"context"
	"reflect"

	"go.uber.org/zap"
)

type RowTransformer struct {
	logger *zap.Logger
}

func NewRowTransformer(log *zap.Logger) *RowTransformer {
	return &RowTransformer{
		logger: log,
	}
}

func (t *RowTransformer) Headers(data map[string]interface{}) []string {
	headers := make([]string, len(data))

	i := 0
	for k := range data {
		headers[i] = k
		i += 1
	}

	return headers
}

func (t *RowTransformer) Transform(ctx context.Context, data map[string]interface{}, fields []string) ([][]string, error) {
	return mapTo2DSlice(data, fields, maxRowsCount(data)), nil
}

func maxRowsCount(m map[string]interface{}) int {
	rc := 1

	for _, v := range m {
		if isListType(v) {
			sv := v.([]interface{})
			if len(sv) > rc {
				rc = len(sv)
			}
		}
	}

	return rc
}

func mapTo2DSlice(m map[string]interface{}, fields []string, rowsCount int) [][]string {
	arr := make([][]string, rowsCount)
	for row := 0; row < rowsCount; row++ {
		arr[row] = make([]string, len(fields))
		for col, field := range fields {
			v := m[field]
			if isListType(v) {
				va := v.([]interface{})
				if row < len(va) {
					arr[row][col] = va[row].(string)
				} else {
					arr[row][col] = ""
				}
			} else {
				arr[row][col] = v.(string)
			}
		}
	}

	return arr
}

func isListType(v interface{}) bool {
	rt := reflect.TypeOf(v)
	return rt.Kind() == reflect.Array || rt.Kind() == reflect.Slice
}
