package custom

import (
	"errors"
	"fmt"
	"strings"
)

type Stack[T comparable] struct {
	values []T
	ptr    int
	size   int
}

// New creates a new Stack with the given values.
func NewStack[T comparable](ss ...T) *Stack[T] {
	return &Stack[T]{
		values: ss,
		size:   len(ss),
	}
}

// Empty returns true if the stack is empty.
func (s *Stack[T]) Empty() bool {
	return s.size == 0
}

// Size returns the stack size.
func (s *Stack[T]) Size() int {
	return s.size
}

// Pop removes and returns the top element of the stack.
func (s *Stack[T]) Pop() (T, error) {
	var t T
	if s.Empty() {
		return t, errors.New("empty stack")
	}

	v := s.values[s.size-1]

	s.values = s.values[:s.size-1]

	s.size -= 1

	return v, nil
}

// PopN removes n elements from the stack.
func (s *Stack[T]) PopN(n int) ([]T, error) {
	if s.Empty() {
		return nil, errors.New("empty stack")
	}

	if s.size < n {
		return nil, errors.New(fmt.Sprintf("stack size [%d] less than requested [%d]", s.size, n))
	}

	popIdx := s.size - n

	vals := s.values[popIdx:]

	s.values = s.values[:popIdx]

	s.size -= n

	return vals, nil
}

// Push adds values to the stack.
func (s *Stack[T]) Push(vals ...T) {
	s.values = append(s.values, vals...)

	s.size += len(vals)
}

// Peek returns the top element of the stack.
func (s *Stack[T]) Peek() T {
	var t T
	if s.Empty() {
		return t
	}

	return s.values[s.size-1]
}

// Print returns a string representation of the stack
func (s *Stack[T]) String() string {
	var sb strings.Builder

	sb.WriteString("[")
	for _, v := range s.values {
		ws := fmt.Sprintf(" %v", v)
		sb.WriteString(ws)
	}
	sb.WriteString(" ]")

	return sb.String()
}
