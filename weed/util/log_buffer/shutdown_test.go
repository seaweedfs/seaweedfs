package log_buffer

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestShutdownDrainsAcceptedRecords(t *testing.T) {
	for _, seal := range []string{"append", "interval", "force"} {
		t.Run(seal, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				releaseFlush := make(chan struct{})
				var persisted bytes.Buffer
				lb := NewLogBuffer("shutdown", time.Hour, func(_ *LogBuffer, _, _ time.Time, data []byte, _, _ int64) {
					<-releaseFlush
					persisted.Write(data)
				}, nil, nil)

				// Fill the queue behind a blocked persistence call, leaving one
				// record in the current window. Each append seals its predecessor.
				var want []string
				appendRecord := func() {
					value := fmt.Sprint(len(want))
					ts := time.Now().Add(time.Duration(len(want)) * 2 * time.Hour).UnixNano()
					if err := lb.AddDataToBuffer(nil, []byte(value), ts); err != nil {
						t.Error(err)
					}
					want = append(want, value)
				}
				for range flushQueueDepth + 2 {
					appendRecord()
					synctest.Wait()
				}
				switch seal {
				case "append":
					go appendRecord()
				case "interval":
					time.Sleep(time.Hour)
				case "force":
					go lb.ForceFlush()
				}
				synctest.Wait()

				done := make(chan struct{})
				go func() {
					lb.ShutdownLogBuffer()
					lb.WaitForShutdown()
					close(done)
				}()
				<-lb.shutdownCh
				select {
				case <-done:
					t.Error("shutdown completed while persistence was blocked")
				default:
				}

				close(releaseFlush)
				<-done
				synctest.Wait()
				var got []string
				for persisted.Len() > 0 {
					size := util.BytesToUint32(persisted.Next(4))
					var entry filer_pb.LogEntry
					if err := entry.UnmarshalVT(persisted.Next(int(size))); err != nil {
						t.Fatal(err)
					}
					got = append(got, string(entry.Data))
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("persisted records = %v, want %v", got, want)
				}
			})
		})
	}
}

func TestShutdownRejectsNewRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lb := NewLogBuffer("stopped", time.Hour, nil, nil, nil)
		lb.ShutdownLogBuffer()
		for _, appendRecord := range []func() error{
			func() error { return lb.AddDataToBuffer(nil, []byte("late data"), 0) },
			func() error { return lb.AddLogEntryToBuffer(&filer_pb.LogEntry{Data: []byte("late entry")}) },
		} {
			if err := appendRecord(); !errors.Is(err, ErrBufferStopped) {
				t.Errorf("append after shutdown = %v, want ErrBufferStopped", err)
			}
		}
		lb.ForceFlush()
		lb.WaitForShutdown()
		if lb.pos != 0 {
			t.Errorf("stopped buffer retained %d bytes", lb.pos)
		}
	})
}

func TestWaitForShutdownWithEmptyBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lb := NewLogBuffer("empty", time.Hour, nil, nil, nil)
		lb.ShutdownLogBuffer()
		lb.WaitForShutdown()
		lb.WaitForShutdown()
	})
}
