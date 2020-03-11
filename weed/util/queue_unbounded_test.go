package util

import "testing"

func TestEnqueueAndConsume(t *testing.T) {

	q := NewUnboundedQueue()

	q.EnQueue("1", "2", "3")

	f := func(items []string) {
		for _, t := range items {
			println(t)
		}
		println("-----------------------")
	}
	q.Consume(f)

	q.Consume(f)

	q.EnQueue("4", "5")
	q.EnQueue("6", "7")
	q.Consume(f)

}
