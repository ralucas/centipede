package etl

import (
	"context"
	"io"
)

type Loader interface {
	Load(ctx context.Context, data [][]string, writer io.Writer) error
}
