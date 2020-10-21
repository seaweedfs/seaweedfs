package util

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
