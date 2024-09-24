package streamreader

import (
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"

	"github.com/ralucas/centipede/internal/schema"
	"github.com/ralucas/centipede/pkg/etl"

	"go.uber.org/zap"
)

var ErrInvalidDatasetJSON = errors.New("json does not conform to dataset schema")

type JSONStreamIterator struct {
	reader      io.Reader
	logger      *zap.Logger
	hasNext     *atomic.Bool
	chunkSize   int
	dec         *json.Decoder
	validate    bool
	initialized *atomic.Bool
}

type JSONStreamIteratorOption func(*JSONStreamIterator)

// WithDatasetValidation uses the published schema from
// https://project-open-data.cio.gov/v1.1/schema/dataset.json
// to validate against.
func WithDatasetValidation() JSONStreamIteratorOption {
	return func(r *JSONStreamIterator) {
		r.validate = true
	}
}

func NewJSONStreamIterator(reader io.Reader, log *zap.Logger, opts ...JSONStreamIteratorOption) *JSONStreamIterator {
	r := &JSONStreamIterator{
		reader:      reader,
		logger:      log,
		hasNext:     &atomic.Bool{},
		dec:         json.NewDecoder(reader),
		initialized: &atomic.Bool{},
	}
	// we start with the assumption it has at least one item in the array
	r.hasNext.Store(true)

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *JSONStreamIterator) initialize() error {
	if !r.initialized.Load() {
		t, err := r.dec.Token()
		if err != nil {
			r.logger.Error("failed to decode opening array bracket", zap.Any("opener", t), zap.Error(err))
			return err
		}
	// find root key, handle if it doesn't exist
	}

	r.initialized.Store(true)
	return nil
}

// Iterator Pattern to get next json object. On error it will mark
// HasNext as false.
func (r *JSONStreamIterator) Next() (map[string]interface{}, error) {
	// set hasNext to false on each call, which will cause iteration
	// to end on first error
	r.hasNext.Store(false)

	// check if it's been initialized
	if !r.initialized.Load() {
		err := r.initialize()
		if err != nil {
			return nil, err
		}
	}

	if r.dec.More() {
		var m map[string]interface{}
		err := r.dec.Decode(&m)
		if err != nil {
			r.logger.Error("failed to decode", zap.Error(err))
			return nil, err
		}
		if r.validate {
			if ok, err := r.validateDataset(m); !ok {
				return nil, errors.Join(ErrInvalidDatasetJSON, err)
			}
		}

		r.hasNext.Store(true)
		return m, nil
	}

	return nil, etl.Done
}

func (r *JSONStreamIterator) HasNext() bool {
	return r.hasNext.Load()
}

func (r *JSONStreamIterator) validateDataset(m map[string]interface{}) (bool, error) {
	d := &schema.DatasetJson{}

	b, err := json.Marshal(m)
	if err != nil {
		return false, err
	}

	if err = d.UnmarshalJSON(b); err != nil {
		return false, err
	}

	return true, nil
}
