package buffered_queue

import (
	"sync"
	"testing"
)

func TestJobQueue(t *testing.T) {
	type Job[T any] struct {
		ID     int
		Action string
		Data   T
	}

	queue := NewBufferedQueue[Job[string]](2) // Chunk size of 5
	queue.Enqueue(Job[string]{ID: 1, Action: "task1", Data: "hello"})
	queue.Enqueue(Job[string]{ID: 2, Action: "task2", Data: "world"})

	if queue.Size() != 2 {
		t.Errorf("Expected queue size of 2, got %d", queue.Size())
	}

	queue.Enqueue(Job[string]{ID: 3, Action: "task3", Data: "3!"})
	queue.Enqueue(Job[string]{ID: 4, Action: "task4", Data: "4!"})
	queue.Enqueue(Job[string]{ID: 5, Action: "task5", Data: "5!"})

	if queue.Size() != 5 {
		t.Errorf("Expected queue size of 5, got %d", queue.Size())
	}

	println("enqueued 5 items")

	println("dequeue", 1)
	job, ok := queue.Dequeue()
	if !ok {
		t.Errorf("Expected dequeue to return true")
	}
	if job.ID != 1 {
		t.Errorf("Expected job ID of 1, got %d", job.ID)
	}

	println("dequeue", 2)
	job, ok = queue.Dequeue()
	if !ok {
		t.Errorf("Expected dequeue to return true")
	}

	println("enqueue", 6)
	queue.Enqueue(Job[string]{ID: 6, Action: "task6", Data: "6!"})
	println("enqueue", 7)
	queue.Enqueue(Job[string]{ID: 7, Action: "task7", Data: "7!"})

	for i := 0; i < 5; i++ {
		println("dequeue ...")
		job, ok = queue.Dequeue()
		if !ok {
			t.Errorf("Expected dequeue to return true")
		}
		println("dequeued", job.ID)
	}

	if queue.Size() != 0 {
		t.Errorf("Expected queue size of 0, got %d", queue.Size())
	}

	for i := 0; i < 5; i++ {
		println("enqueue", i+8)
		queue.Enqueue(Job[string]{ID: i + 8, Action: "task", Data: "data"})
	}
	for i := 0; i < 5; i++ {
		job, ok = queue.Dequeue()
		if !ok {
			t.Errorf("Expected dequeue to return true")
		}
		if job.ID != i+8 {
			t.Errorf("Expected job ID of %d, got %d", i, job.ID)
		}
		println("dequeued", job.ID)
	}

}

func TestJobQueueClose(t *testing.T) {
	type Job[T any] struct {
		ID     int
		Action string
		Data   T
	}

	queue := NewBufferedQueue[Job[string]](2)
	queue.Enqueue(Job[string]{ID: 1, Action: "task1", Data: "hello"})
	queue.Enqueue(Job[string]{ID: 2, Action: "task2", Data: "world"})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for data, ok := queue.Dequeue(); ok; data, ok = queue.Dequeue() {
			println("dequeued", data.ID)
		}
	}()

	for i := 0; i < 5; i++ {
		queue.Enqueue(Job[string]{ID: i + 3, Action: "task", Data: "data"})
	}

	queue.CloseInput()
	wg.Wait()

}

func BenchmarkBufferedQueue(b *testing.B) {
	type Job[T any] struct {
		ID     int
		Action string
		Data   T
	}

	queue := NewBufferedQueue[Job[string]](1024)

	for i := 0; i < b.N; i++ {
		queue.Enqueue(Job[string]{ID: i, Action: "task", Data: "data"})
	}
	for i := 0; i < b.N; i++ {
		_, _ = queue.Dequeue()
	}

}
