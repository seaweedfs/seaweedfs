//go:build !plan9

package grace

import (
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var signalChan chan os.Signal
var interruptHooks = make([]func(), 0)
var firstInterruptHooks = make([]func(), 0)
var interruptHookLock sync.RWMutex
var reloadHooks = make([]func(), 0)
var reloadHookLock sync.RWMutex

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

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
				runInterruptHooks()
				os.Exit(0)
			}
		}
	}()
}

// runInterruptHooks runs the OnInterruptFirst hooks in registration order,
// then the OnInterrupt hooks in reverse registration order (LIFO/defer-style)
// so later-started services shut down before the services they depend on.
func runInterruptHooks() {
	interruptHookLock.RLock()
	defer interruptHookLock.RUnlock()
	for _, hook := range firstInterruptHooks {
		hook()
	}
	for i := len(interruptHooks) - 1; i >= 0; i-- {
		hook := interruptHooks[i]
		glog.V(4).Infof("exec interrupt hook func name:%s", GetFunctionName(hook))
		hook()
	}
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

// OnInterruptFirst registers fn to run before every OnInterrupt hook,
// whenever those were registered.
func OnInterruptFirst(fn func()) {
	interruptHookLock.Lock()
	defer interruptHookLock.Unlock()
	firstInterruptHooks = append(firstInterruptHooks, fn)
}
