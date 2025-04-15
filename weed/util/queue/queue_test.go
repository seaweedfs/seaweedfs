package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWaiters(t *testing.T) {
	s1, s2, s3, s4 := newSema(), newSema(), newSema(), newSema()

	w := waiters{}
	assert.Len(t, w, 0)

	//
	// test put()
	w.put(s1)
	assert.Equal(t, waiters{s1}, w)

	w.put(s2)
	w.put(s3)
	w.put(s4)
	assert.Equal(t, waiters{s1, s2, s3, s4}, w)

	//
	// test remove()
	//
	// remove from middle
	w.remove(s2)
	assert.Equal(t, waiters{s1, s3, s4}, w)

	// remove non-existing element
	w.remove(s2)
	assert.Equal(t, waiters{s1, s3, s4}, w)

	// remove from beginning
	w.remove(s1)
	assert.Equal(t, waiters{s3, s4}, w)

	// remove from end
	w.remove(s4)
	assert.Equal(t, waiters{s3}, w)

	// remove last element
	w.remove(s3)
	assert.Empty(t, w)

	// remove non-existing element
	w.remove(s3)
	assert.Empty(t, w)

	//
	// test get()
	//
	// start with 3 elements in list
	w.put(s1)
	w.put(s2)
	w.put(s3)
	assert.Equal(t, waiters{s1, s2, s3}, w)

	// get() returns each item in insertion order
	assert.Equal(t, s1, w.get())
	assert.Equal(t, s2, w.get())
	w.put(s4) // interleave a put(), item should go to the end
	assert.Equal(t, s3, w.get())
	assert.Equal(t, s4, w.get())
	assert.Empty(t, w)
	assert.Nil(t, w.get())
}
