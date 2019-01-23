package util

import "runtime"

func GoMaxProcs(maxCPU *int) {
	if *maxCPU < 1 {
		*maxCPU = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*maxCPU)
}
