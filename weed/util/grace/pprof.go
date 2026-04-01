package grace

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// StartDebugServer starts an HTTP server for pprof debugging on the specified port.
// The server runs in a goroutine and serves pprof endpoints at /debug/pprof/*.
func StartDebugServer(debugPort int) {
	go func() {
		addr := fmt.Sprintf(":%d", debugPort)
		glog.V(0).Infof("Starting debug server for pprof at http://localhost%s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
			glog.Errorf("Failed to start debug server on %s: %v", addr, err)
		}
	}()
}

func SetupProfiling(cpuProfile, memProfile string) {
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			glog.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		pprof.StartCPUProfile(f)
		OnInterrupt(func() {
			pprof.StopCPUProfile()

			// write block pprof
			blockF, err := os.Create(cpuProfile + ".block")
			if err != nil {
				return
			}
			p := pprof.Lookup("block")
			p.WriteTo(blockF, 0)
			blockF.Close()

			// write mutex pprof
			mutexF, err := os.Create(cpuProfile + ".mutex")
			if err != nil {
				return
			}
			p = pprof.Lookup("mutex")
			p.WriteTo(mutexF, 0)
			mutexF.Close()

		})
	}
	if memProfile != "" {
		runtime.MemProfileRate = 1
		f, err := os.Create(memProfile)
		if err != nil {
			glog.Fatal(err)
		}
		OnInterrupt(func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		})
	}

}
