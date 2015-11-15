// +build !plan9

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func OnInterrupt(fn func()) {
	// Deal with control+c,etc

	// Controlling terminal close, daemon not exit
	ignoreChan := make(chan os.Signal, 1)
	signal.Notify(ignoreChan, syscall.SIGHUP)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		// syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		for {
			select {
			case <-signalChan:
				fn()
				os.Exit(0)

			case <-ignoreChan:
				// Ignore
				break
			}
		}
	}()
}
