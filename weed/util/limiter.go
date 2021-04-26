package util

import (
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
)

// initial version comes from https://github.com/korovkin/limiter/blob/master/limiter.go

// LimitedConcurrentExecutor object
type LimitedConcurrentExecutor struct {
	limit     int
	tokenChan chan int
}

func NewLimitedConcurrentExecutor(limit int) *LimitedConcurrentExecutor {

	// allocate a limiter instance
	c := &LimitedConcurrentExecutor{
		limit:     limit,
		tokenChan: make(chan int, limit),
	}

	// allocate the tokenChan:
	for i := 0; i < c.limit; i++ {
		c.tokenChan <- i
	}

	return c
}

// Execute adds a function to the execution queue.
// if num of go routines allocated by this instance is < limit
// launch a new go routine to execute job
// else wait until a go routine becomes available
func (c *LimitedConcurrentExecutor) Execute(job func()) {
	token := <-c.tokenChan
	go func() {
		defer func() {
			c.tokenChan <- token
		}()
		// run the job
		job()
	}()
}

// a different implementation, but somehow more "conservative"
type OperationRequest func()

type LimitedOutOfOrderProcessor struct {
	processorSlots     uint32
	processors         []chan OperationRequest
	processorLimit     int32
	processorLimitCond *sync.Cond
	currentProcessor   int32
}

func NewLimitedOutOfOrderProcessor(limit int32) (c *LimitedOutOfOrderProcessor) {

	processorSlots := uint32(32)
	c = &LimitedOutOfOrderProcessor{
		processorSlots:     processorSlots,
		processors:         make([]chan OperationRequest, processorSlots),
		processorLimit:     limit,
		processorLimitCond: sync.NewCond(new(sync.Mutex)),
	}

	for i := 0; i < int(processorSlots); i++ {
		c.processors[i] = make(chan OperationRequest)
	}

	cases := make([]reflect.SelectCase, processorSlots)
	for i, ch := range c.processors {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	go func() {
		for {
			_, value, ok := reflect.Select(cases)
			if !ok {
				continue
			}

			request := value.Interface().(OperationRequest)

			if c.processorLimit > 0 {
				c.processorLimitCond.L.Lock()
				for atomic.LoadInt32(&c.currentProcessor) > c.processorLimit {
					c.processorLimitCond.Wait()
				}
				atomic.AddInt32(&c.currentProcessor, 1)
				c.processorLimitCond.L.Unlock()
			}

			go func() {
				if c.processorLimit > 0 {
					defer atomic.AddInt32(&c.currentProcessor, -1)
					defer c.processorLimitCond.Signal()
				}
				request()
			}()

		}
	}()

	return c
}

func (c *LimitedOutOfOrderProcessor) Execute(request OperationRequest) {
	index := rand.Uint32() % c.processorSlots
	c.processors[index] <- request
}
