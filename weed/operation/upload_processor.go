package operation

import (
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

type OperationRequest func()

var (
	requestSlots          = uint32(32)
	requests              = make([]chan OperationRequest, requestSlots) // increase slots to increase fairness
	ConcurrentUploadLimit = int32(runtime.NumCPU())                     // directly related to memory usage
	concurrentLimitCond   = sync.NewCond(new(sync.Mutex))
	concurrentUpload      int32
)

func init() {

	for i := 0; i < int(requestSlots); i++ {
		requests[i] = make(chan OperationRequest)
	}

	cases := make([]reflect.SelectCase, requestSlots)
	for i, ch := range requests {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	go func() {
		for {
			_, value, ok := reflect.Select(cases)
			if !ok {
				continue
			}

			request := value.Interface().(OperationRequest)

			concurrentLimitCond.L.Lock()
			for atomic.LoadInt32(&concurrentUpload) > ConcurrentUploadLimit {
				concurrentLimitCond.Wait()
			}
			atomic.AddInt32(&concurrentUpload, 1)
			concurrentLimitCond.L.Unlock()

			go func() {
				defer atomic.AddInt32(&concurrentUpload, -1)
				defer concurrentLimitCond.Signal()
				request()
			}()

		}
	}()

}

func AsyncOutOfOrderProcess(slotKey uint32, request OperationRequest) {
	index := slotKey % requestSlots
	requests[index] <- request
}
