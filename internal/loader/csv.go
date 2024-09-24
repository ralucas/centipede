package loader

import (
	"context"
	"encoding/csv"
	"io"
	"sync"

	"go.uber.org/zap"
)

type CSVLoader struct {
	mu     *sync.Mutex
	logger *zap.Logger
}

func NewCSVLoader(log *zap.Logger) *CSVLoader {
	return &CSVLoader{
		mu:     &sync.Mutex{},
		logger: log,
	}
}

func (l *CSVLoader) Load(ctx context.Context, records [][]string, file io.Writer) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Debug("loading records to csv")

	w := csv.NewWriter(file)

	for _, record := range records {
		if err := w.Write(record); err != nil {
			l.logger.Error("failed writing record to csv", zap.Any("record", record), zap.Error(err))
			return err
		}
	}

	w.Flush()

	if err := w.Error(); err != nil {
		l.logger.Error("failed to write/flush", zap.Error(err))
		return err
	}

	l.logger.Debug("successfully loaded records to csv")

	return nil
}
