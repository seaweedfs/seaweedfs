//go:build !plan9
// +build !plan9

package grace

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var signalChan chan os.Signal
var hooks = make([]func(), 0)
var hookLock sync.Mutex

func init() {
	signalChan = make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGHUP)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		// syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		// syscall.SIGQUIT,
	)
	go func() {
		for _ = range signalChan {
			for _, hook := range hooks {
				hook()
			}
			os.Exit(0)
		}
	}()
}

func OnInterrupt(fn func()) {
	// prevent reentry
	hookLock.Lock()
	defer hookLock.Unlock()

	// deal with control+c,etc
	// controlling terminal close, daemon not exit
	hooks = append(hooks, fn)
}
