/*
Copyright 2014 Workiva, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityPut(t *testing.T) {
	q := NewPriorityQueue(1, false)

	q.Put(mockItem(2))

	assert.Len(t, q.items, 1)
	assert.Equal(t, mockItem(2), q.items[0])

	q.Put(mockItem(1))

	if !assert.Len(t, q.items, 2) {
		return
	}
	assert.Equal(t, mockItem(1), q.items[0])
	assert.Equal(t, mockItem(2), q.items[1])
}

func TestPriorityGet(t *testing.T) {
	q := NewPriorityQueue(1, false)

	q.Put(mockItem(2))
	result, err := q.Get(2)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(2), result[0])
	assert.Len(t, q.items, 0)

	q.Put(mockItem(2))
	q.Put(mockItem(1))

	result, err = q.Get(1)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(1), result[0])
	assert.Len(t, q.items, 1)

	result, err = q.Get(2)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(2), result[0])
}

func TestAddEmptyPriorityPut(t *testing.T) {
	q := NewPriorityQueue(1, false)

	q.Put()

	assert.Len(t, q.items, 0)
}

func TestPriorityGetNonPositiveNumber(t *testing.T) {
	q := NewPriorityQueue(1, false)

	q.Put(mockItem(1))

	result, err := q.Get(0)
	if !assert.Nil(t, err) {
		return
	}

	assert.Len(t, result, 0)

	result, err = q.Get(-1)
	if !assert.Nil(t, err) {
		return
	}

	assert.Len(t, result, 0)
}

func TestPriorityEmpty(t *testing.T) {
	q := NewPriorityQueue(1, false)
	assert.True(t, q.Empty())

	q.Put(mockItem(1))

	assert.False(t, q.Empty())
}

func TestPriorityGetEmpty(t *testing.T) {
	q := NewPriorityQueue(1, false)

	go func() {
		q.Put(mockItem(1))
	}()

	result, err := q.Get(1)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Len(t, result, 1) {
		return
	}
	assert.Equal(t, mockItem(1), result[0])
}

func TestMultiplePriorityGetEmpty(t *testing.T) {
	q := NewPriorityQueue(1, false)
	var wg sync.WaitGroup
	wg.Add(2)
	results := make([][]Item, 2)

	go func() {
		wg.Done()
		local, _ := q.Get(1)
		results[0] = local
		wg.Done()
	}()

	go func() {
		wg.Done()
		local, _ := q.Get(1)
		results[1] = local
		wg.Done()
	}()

	wg.Wait()
	wg.Add(2)

	q.Put(mockItem(1), mockItem(3), mockItem(2))
	wg.Wait()

	if !assert.Len(t, results[0], 1) || !assert.Len(t, results[1], 1) {
		return
	}

	assert.True(
		t, (results[0][0] == mockItem(1) && results[1][0] == mockItem(2)) ||
			results[0][0] == mockItem(2) && results[1][0] == mockItem(1),
	)
}

func TestEmptyPriorityGetWithDispose(t *testing.T) {
	q := NewPriorityQueue(1, false)
	var wg sync.WaitGroup
	wg.Add(1)

	var err error
	go func() {
		wg.Done()
		_, err = q.Get(1)
		wg.Done()
	}()

	wg.Wait()
	wg.Add(1)

	q.Dispose()

	wg.Wait()

	assert.IsType(t, ErrDisposed, err)
}

func TestPriorityGetPutDisposed(t *testing.T) {
	q := NewPriorityQueue(1, false)
	q.Dispose()

	_, err := q.Get(1)
	assert.IsType(t, ErrDisposed, err)

	err = q.Put(mockItem(1))
	assert.IsType(t, ErrDisposed, err)
}

func BenchmarkPriorityQueue(b *testing.B) {
	q := NewPriorityQueue(b.N, false)
	var wg sync.WaitGroup
	wg.Add(1)
	i := 0

	go func() {
		for {
			q.Get(1)
			i++
			if i == b.N {
				wg.Done()
				break
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.Put(mockItem(i))
	}

	wg.Wait()
}

func TestPriorityPeek(t *testing.T) {
	q := NewPriorityQueue(1, false)
	q.Put(mockItem(1))

	assert.Equal(t, mockItem(1), q.Peek())
}

func TestInsertDuplicate(t *testing.T) {
	q := NewPriorityQueue(1, false)
	q.Put(mockItem(1))
	q.Put(mockItem(1))

	assert.Equal(t, 1, q.Len())
}

func TestAllowDuplicates(t *testing.T) {
	q := NewPriorityQueue(2, true)
	q.Put(mockItem(1))
	q.Put(mockItem(1))

	assert.Equal(t, 2, q.Len())
}
