package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewQueue(t *testing.T) {

	q := NewQueue[int]()

	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}

	assert.Equal(t, q.Len(), 10)

	for i := 0; i < 10; i++ {
		assert.Equal(t, q.Dequeue(), i)
	}

}
