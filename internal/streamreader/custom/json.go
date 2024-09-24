package custom

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/ralucas/centipede/internal/schema"
	"github.com/ralucas/centipede/internal/streamreader"
	"github.com/ralucas/centipede/pkg/etl"

	"go.uber.org/zap"
)

var (
	ErrIncompleteJSON = errors.New("json is incomplete")
	ErrJSONArray      = errors.New("json is an array")
)

const defaultChunkSize int = 2048

type CustomJSONStreamReadIterator struct {
	reader    io.Reader
	logger    *zap.Logger
	stack     *Stack[byte]
	cur       []byte
	next      []byte
	nextStart int
	hasNext   bool
	isArray   bool
	chunkSize int
	eof       bool
	validate  bool
}

type JSONStreamReadIteratorOption func(*CustomJSONStreamReadIterator)

func WithChunkSize(size int) JSONStreamReadIteratorOption {
	return func(r *CustomJSONStreamReadIterator) {
		if size <= 0 {
			r.logger.Info("size is 0 or less, ignoring, setting to default", zap.Int("defaultChunkSize", defaultChunkSize))
			r.chunkSize = defaultChunkSize
		} else {
			r.chunkSize = size
		}
	}
}

// WithDatasetValidation uses the published schema from
// https://project-open-data.cio.gov/v1.1/schema/dataset.json
// to validate against.
func WithDatasetValidation() JSONStreamReadIteratorOption {
	return func(r *CustomJSONStreamReadIterator) {
		r.validate = true
	}
}

func NewCustomJSONStreamReadIterator(reader io.Reader, log *zap.Logger, opts ...JSONStreamReadIteratorOption) *CustomJSONStreamReadIterator {
	r := &CustomJSONStreamReadIterator{
		reader:    reader,
		logger:    log,
		chunkSize: defaultChunkSize,
		hasNext:   true,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Read takes in a chunked byte stream and returns valid JSON bytes or an error.
func (r *CustomJSONStreamReadIterator) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.eof = true
		} else {
			return n, err
		}
	}

	i := 0

	var token byte

	b = append(r.next, b...)

	if r.stack == nil || r.stack.Empty() {
		// obtain the start of the json object
		for i < len(b) {
			token = b[i]
			if token == '{' {
				r.nextStart = i
				i += 1
				break
			}
			if token == '[' {
				r.isArray = true
			}
			i += 1
		}

		r.stack = NewStack(token)
	}

	for i < len(b) {
		token = b[i]
		if matches(r.stack.Peek(), token) {
			r.stack.Pop()
		} else if isDelim(token) {
			r.stack.Push(token)
		}

		if r.stack.Empty() {
			var e error
			if r.eof {
				e = io.EOF
			}
			return i + 1 - len(r.next), e
		}
		i += 1
	}

	if r.eof {
		return n, io.EOF
	}
	return n, ErrIncompleteJSON
}

// Iterator Pattern to get next json object
func (r *CustomJSONStreamReadIterator) Next() (map[string]interface{}, error) {
	totalBytes := r.next

	for {
		b := make([]byte, r.chunkSize)
		n, err := r.Read(b)

		if totalBytes != nil {
			if n >= 0 {
				totalBytes = append(totalBytes[r.nextStart:], b[:n]...)
			} else {
				end := len(totalBytes) + n
				totalBytes = totalBytes[r.nextStart:end]
			}
		} else {
			totalBytes = b[r.nextStart:n]
		}

		if errors.Is(err, io.EOF) {
			r.hasNext = false
			if n == 0 {
				return nil, etl.Done
			}
			if len(totalBytes) > 0 {
				r.cur = totalBytes
				return r.toMap(totalBytes)
			}
			return nil, etl.Done
		}

		if errors.Is(err, ErrIncompleteJSON) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if n > 0 && len(b) > n && b[n] != '\x00' {
			r.hasNext = true
			r.next = b[n:]
		}
		break
	}

	r.cur = totalBytes
	return r.toMap(totalBytes)
}

func (r *CustomJSONStreamReadIterator) HasNext() bool {
	return r.hasNext
}

func (r *CustomJSONStreamReadIterator) IsArray() bool {
	return r.isArray
}

func (r *CustomJSONStreamReadIterator) toMap(data []byte) (map[string]interface{}, error) {
	if r.validate {
		d := &schema.DatasetJson{}
		if err := d.UnmarshalJSON(data); err != nil {
			r.logger.Error("failed validation", zap.Error(err))
			return nil, errors.Join(streamreader.ErrInvalidDatasetJSON, err)
		}
	}

	var m map[string]interface{}

	err := json.Unmarshal(data, &m)
	if err != nil {
		r.logger.Error("failed to unmarshal to map", zap.Error(err))
		return nil, err
	}

	return m, nil
}

func isDelim(token byte) bool {
	return token == '[' || token == ']' || token == '{' || token == '}'
}

func matches(open, close byte) bool {
	switch open {
	case '[':
		return close == ']'
	case '{':
		return close == '}'
	default:
		return false
	}
}
