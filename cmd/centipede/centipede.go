package centipede

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/oklog/run"
	"github.com/ralucas/centipede/internal/extractor"
	"github.com/ralucas/centipede/internal/loader"
	"github.com/ralucas/centipede/internal/streamreader"
	"github.com/ralucas/centipede/internal/streamreader/custom"
	"github.com/ralucas/centipede/internal/transformer"
	"github.com/ralucas/centipede/pkg/etl"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	Verbose         bool
	Validate        bool
	ChunkSize       int
	UseCustomParser bool
}

func newLogger(level zapcore.Level) *zap.Logger {
	lvl := zap.NewAtomicLevel()
	logger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.Lock(os.Stdout),
		lvl,
	))

	lvl.SetLevel(level)

	return logger
}

func Run(inputFile, outputFile string, fields []string, conf Config) error {
	level := zapcore.InfoLevel
	if conf.Verbose {
		level = zapcore.DebugLevel
	}

	logger := newLogger(level)

	// syncing the logger flushes any buffered log entries.
	defer logger.Sync()

	logger.Debug("logging initialized")

	// create new context for use throughout the application.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Centripede is running...")

	// Get file descriptors for the input and output files
	input, err := os.Open(inputFile)
	if err != nil {
		logger.Error("failed to read input file", zap.Error(err))
		return err
	}

	defer input.Close()

	_, err = os.Stat(outputFile)
	if os.IsExist(err) {
		logger.Error("output file already exists")
		return err
	}

	output, err := os.Create(outputFile)
	if err != nil {
		logger.Error("failed to create output file", zap.String("name", output.Name()), zap.Error(err))
		return err
	}

	defer output.Close()

	var si etl.StreamIterator

	if conf.UseCustomParser {
		var custOpts []custom.JSONStreamReadIteratorOption
		if conf.Validate {
			custOpts = append(custOpts, custom.WithDatasetValidation())
		}
		si = custom.NewCustomJSONStreamReadIterator(input, logger, custOpts...)
	} else {
		var readerOpts []streamreader.JSONStreamIteratorOption
		if conf.Validate {
			readerOpts = append(readerOpts, streamreader.WithDatasetValidation())
		}

		si = streamreader.NewJSONStreamIterator(input, logger, readerOpts...)
	}

	processor := etl.NewETLProcessor(
		extractor.NewMapExtractor(logger),
		transformer.NewRowTransformer(logger),
		loader.NewCSVLoader(logger),
		si,
		logger,
	)

	// Run groups provide an easy way to manage multiple goroutines
	var g run.Group

	g.Add(func() error {
		logger.Info(fmt.Sprintf("Running the etl process from %s to %s", input.Name(), output.Name()))
		return processor.Process(ctx, output, fields)
	}, func(err error) {
		if err != nil {
			logger.Error("error in ETL processor, shutting down", zap.Error(err))
			cancel()
		}
	})

	// Create a watcher to handle SIGTERM or SIGINT and trigger graceful shutdown
	signalc := make(chan os.Signal, 1)
	signal.Notify(signalc, syscall.SIGTERM, syscall.SIGINT)

	g.Add(func() error {
		sig := <-signalc
		if sig != nil {
			cancel()
			return fmt.Errorf("Received %v signal; shutting down service", sig.String())
		}
		return nil
	}, func(err error) {
		defer close(signalc)
	})

	return g.Run()
}
