package etl

import "errors"

var Done = errors.New("iterator done")

type StreamIterator interface {
	Next() (map[string]interface{}, error)
	HasNext() bool
}
