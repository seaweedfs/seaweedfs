package storage

import (
	"errors"
	"sync"
	"syscall"
)

const IoErrorTolerance = 3

type IoErrorTracker struct {
	lastIoError        error
	lastIoErrorCount   int32
	ioErrorQuarantined bool
	lastIoErrorLock    sync.RWMutex
}

func (t *IoErrorTracker) checkReadWriteError(err error) {
	if err == nil {
		t.clearIoError()
		return
	}
	if errors.Is(err, syscall.EIO) {
		t.noteIoError(err)
		return
	}
	t.clearIoError()
}

func (t *IoErrorTracker) noteIoError(err error) {
	t.lastIoErrorLock.Lock()
	defer t.lastIoErrorLock.Unlock()
	t.lastIoError = err
	t.lastIoErrorCount++
}

func (t *IoErrorTracker) clearIoError() {
	t.lastIoErrorLock.Lock()
	defer t.lastIoErrorLock.Unlock()
	t.lastIoError = nil
	t.lastIoErrorCount = 0
}

func (t *IoErrorTracker) resetIoErrorState() {
	t.lastIoErrorLock.Lock()
	defer t.lastIoErrorLock.Unlock()
	t.lastIoError = nil
	t.lastIoErrorCount = 0
	t.ioErrorQuarantined = false
}

func (t *IoErrorTracker) markIoQuarantined() {
	t.lastIoErrorLock.Lock()
	defer t.lastIoErrorLock.Unlock()
	t.ioErrorQuarantined = true
}

func (t *IoErrorTracker) getIoErrorState() (error, int32, bool) {
	t.lastIoErrorLock.RLock()
	defer t.lastIoErrorLock.RUnlock()
	return t.lastIoError, t.lastIoErrorCount, t.ioErrorQuarantined
}
