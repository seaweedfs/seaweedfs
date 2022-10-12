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
var interruptHooks = make([]func(), 0)
var interruptHookLock sync.RWMutex
var reloadHooks = make([]func(), 0)
var reloadHookLock sync.RWMutex

func init() {
	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		// syscall.SIGQUIT,
	)
	go func() {
		for s := range signalChan {
			if s.String() == syscall.SIGHUP.String() {
				reloadHookLock.RLock()
				for _, hook := range reloadHooks {
					hook()
				}
				reloadHookLock.RUnlock()
			} else {
				interruptHookLock.RLock()
				for _, hook := range interruptHooks {
					hook()
				}
				interruptHookLock.RUnlock()
				os.Exit(0)
			}
		}
	}()
}

func OnReload(fn func()) {
	// prevent reentry
	reloadHookLock.Lock()
	defer reloadHookLock.Unlock()
	reloadHooks = append(reloadHooks, fn)
}

func OnInterrupt(fn func()) {
	// prevent reentry
	interruptHookLock.Lock()
	defer interruptHookLock.Unlock()

	// deal with control+c,etc
	// controlling terminal close, daemon not exit
	interruptHooks = append(interruptHooks, fn)
}
