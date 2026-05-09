package storage

import (
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// eioBackend is a backend.BackendStorageFile that returns EIO from
// every ReadAt — used to verify the streaming read path threads
// errors through checkReadWriteError.
type eioBackend struct{}

func (eioBackend) ReadAt(p []byte, off int64) (int, error) { return 0, syscall.EIO }
func (eioBackend) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}
func (eioBackend) Truncate(int64) error                          { return nil }
func (eioBackend) Close() error                                  { return nil }
func (eioBackend) GetStat() (int64, time.Time, error)            { return 0, time.Time{}, nil }
func (eioBackend) Name() string                                  { return "eio" }
func (eioBackend) Sync() error                                   { return nil }

func TestCheckReadWriteErrorTracksConsecutiveEIO(t *testing.T) {
	v := &Volume{}

	// each EIO bumps the counter.
	for i := int32(1); i <= 5; i++ {
		v.checkReadWriteError(fmt.Errorf("disk failed: %w", syscall.EIO))
		_, count, _ := v.getIoErrorState()
		if count != i {
			t.Fatalf("after %d EIO(s): counter = %d, want %d", i, count, i)
		}
	}

	// a single success resets the streak (but not the quarantine flag,
	// which is not set here).
	v.checkReadWriteError(nil)
	if err, count, q := v.getIoErrorState(); err != nil || count != 0 || q {
		t.Fatalf("success did not reset state: err=%v count=%d quarantined=%v", err, count, q)
	}
}

func TestCheckReadWriteErrorNonEIOResetsStreak(t *testing.T) {
	v := &Volume{}

	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count, _ := v.getIoErrorState(); count != 2 {
		t.Fatalf("expected count=2 after two EIOs, got %d", count)
	}

	// a non-EIO error breaks the streak — only sustained EIOs are
	// diagnostic of a failing disk.
	v.checkReadWriteError(fmt.Errorf("other: %w", syscall.ENOSPC))
	if err, count, _ := v.getIoErrorState(); err != nil || count != 0 {
		t.Fatalf("non-EIO did not reset streak: err=%v count=%d", err, count)
	}

	// a fresh EIO starts the streak from 1, not 3.
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count, _ := v.getIoErrorState(); count != 1 {
		t.Fatalf("EIO after non-EIO did not restart streak: count=%d, want 1", count)
	}
}

func TestCheckReadWriteErrorIgnoresPlainError(t *testing.T) {
	v := &Volume{}

	v.checkReadWriteError(errors.New("some other error"))
	if err, count, _ := v.getIoErrorState(); err != nil || count != 0 {
		t.Fatalf("non-EIO with no prior streak set state: err=%v count=%d", err, count)
	}
}

func TestIoErrorToleranceGate(t *testing.T) {
	v := &Volume{}

	for i := 0; i < IoErrorTolerance-1; i++ {
		v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	}
	if _, count, _ := v.getIoErrorState(); count >= IoErrorTolerance {
		t.Fatalf("counter %d already crossed tolerance %d after %d errors",
			count, IoErrorTolerance, IoErrorTolerance-1)
	}

	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count, _ := v.getIoErrorState(); count < IoErrorTolerance {
		t.Fatalf("counter %d below tolerance %d after %d errors",
			count, IoErrorTolerance, IoErrorTolerance)
	}
}

// Once CollectHeartbeat marks a replica quarantined, a stray successful
// read must NOT silently put a known-bad disk back into rotation. Only
// MarkVolumeWritable (resetIoErrorState) clears the sticky bit.
func TestQuarantineIsSticky(t *testing.T) {
	v := &Volume{}

	for i := 0; i < IoErrorTolerance; i++ {
		v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	}
	v.markIoQuarantined()

	if _, _, q := v.getIoErrorState(); !q {
		t.Fatalf("markIoQuarantined did not set the flag")
	}

	// A successful read clears the streak counter…
	v.checkReadWriteError(nil)
	if _, count, q := v.getIoErrorState(); count != 0 {
		t.Fatalf("success did not clear streak: count=%d", count)
	} else if !q {
		t.Fatalf("success cleared the sticky quarantine flag — operator-only recovery violated")
	}

	// …and a non-EIO error also clears the streak but not the flag.
	v.checkReadWriteError(fmt.Errorf("other: %w", syscall.ENOSPC))
	if _, _, q := v.getIoErrorState(); !q {
		t.Fatalf("non-EIO cleared the sticky quarantine flag")
	}

	// Only resetIoErrorState (used by MarkVolumeWritable) un-quarantines.
	v.resetIoErrorState()
	if err, count, q := v.getIoErrorState(); err != nil || count != 0 || q {
		t.Fatalf("resetIoErrorState left state: err=%v count=%d quarantined=%v", err, count, q)
	}
}

// Streaming/range reads (ReadNeedleBlob) used to bypass the EIO
// counter, so a failing disk taking range GETs all day would never
// trip IoErrorTolerance. ReadNeedleBlob now threads the backend error
// through checkReadWriteError; one EIO must bump the streak by one.
func TestReadNeedleBlobTracksEIO(t *testing.T) {
	v := &Volume{
		DataBackend: eioBackend{},
		SuperBlock:  super_block.SuperBlock{Version: needle.GetCurrentVersion()},
		volumeInfo:  &volume_server_pb.VolumeInfo{Version: uint32(needle.GetCurrentVersion())},
	}

	if _, err := v.ReadNeedleBlob(0, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("expected EIO from fake backend, got %v", err)
	}
	if _, count, _ := v.getIoErrorState(); count != 1 {
		t.Fatalf("ReadNeedleBlob did not bump EIO streak: count=%d", count)
	}
}

func TestIoErrorStateIsRaceFree(t *testing.T) {
	// Relies on `go test -race` to detect any unprotected access on
	// lastIoError / lastIoErrorCount / ioErrorQuarantined.
	v := &Volume{}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				v.checkReadWriteError(nil)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				v.markIoQuarantined()
				v.resetIoErrorState()
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			v.getIoErrorState()
		}
		close(stop)
	}()
	wg.Wait()
}
