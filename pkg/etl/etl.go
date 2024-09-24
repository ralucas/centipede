package etl

import (
	"context"
	"errors"
	"io"
	"sync"

	"go.uber.org/zap"
)

type ETLProcessor struct {
	streamIterator StreamIterator
	extractor      Extractor
	transformer    Transformer
	loader         Loader
	logger         *zap.Logger
}

func NewETLProcessor(e Extractor, t Transformer, l Loader, si StreamIterator, log *zap.Logger) *ETLProcessor {
	return &ETLProcessor{
		extractor:      e,
		transformer:    t,
		loader:         l,
		streamIterator: si,
		logger:         log,
	}
}

// Process streams file and fans out to the ETL pipelines.
func (e *ETLProcessor) Process(ctx context.Context, output io.Writer, fields []string) error {
	// write the header first
	e.logger.Debug("loading headers")
	e.loader.Load(ctx, [][]string{fields}, output)

	errorc := make(chan error)

	var wg sync.WaitGroup

	for e.streamIterator.HasNext() {
		select {
		case <-ctx.Done():
			e.logger.Info("recieved done, shutting down")
			return nil
		case err := <-errorc:
			e.logger.Error("received error", zap.String("err", err.Error()))
			return err
		default:
			obj, err := e.streamIterator.Next()
			if err != nil {
				if errors.Is(err, Done) {
					e.logger.Debug("done reading")
					break
				} else {
					return err
				}
			}
			wg.Add(1)
			go func(errCh chan error, c context.Context, data map[string]interface{}, fds []string, wo io.Writer) {
				err := e.runPipeline(c, data, fds, wo)
				if err != nil {
					errCh <- err
					return
				}
				wg.Done()
			}(errorc, ctx, obj, fields, output)
		}
	}

	e.logger.Debug("waiting")
	wg.Wait()

	e.logger.Info("ETL process finished")

	return nil
}

func (e *ETLProcessor) runPipeline(ctx context.Context, raw map[string]interface{}, fields []string, output io.Writer) error {
	extract, err := e.extractor.Extract(ctx, raw, fields)
	if err != nil {
		e.logger.Error("failed to extract", zap.Error(err))
		return err
	}

	e.logger.Debug("extracted, transforming...")
	transform, err := e.transformer.Transform(ctx, extract, fields)
	if err != nil {
		e.logger.Error("failed to transform", zap.Error(err))
		return err
	}

	e.logger.Debug("transformed, loading...")
	err = e.loader.Load(ctx, transform, output)
	if err != nil {
		e.logger.Error("failed to load", zap.Error(err))
		return err
	}

	return nil
}
