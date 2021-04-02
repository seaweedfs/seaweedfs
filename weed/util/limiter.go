package util

import (
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
)

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

			c.processorLimitCond.L.Lock()
			for atomic.LoadInt32(&c.currentProcessor) > c.processorLimit {
				c.processorLimitCond.Wait()
			}
			atomic.AddInt32(&c.currentProcessor, 1)
			c.processorLimitCond.L.Unlock()

			go func() {
				defer atomic.AddInt32(&c.currentProcessor, -1)
				defer c.processorLimitCond.Signal()
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
