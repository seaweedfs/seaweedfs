package mount

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	accessReportQueueSize = 4096
	accessReportWorkers   = 8
	// accessReportTimeout bounds one report across all the filers it tries.
	accessReportTimeout = 10 * time.Second
	// accessReportAttemptTimeout bounds the call to one filer, so a filer that
	// accepts the connection but never answers leaves time for the next one.
	accessReportAttemptTimeout = 2 * time.Second
	// accessReportPause is how long reporting stops after a filer answers
	// that it does not implement TouchAccessTime.
	accessReportPause       = 10 * time.Minute
	accessReportLogInterval = time.Minute
)

// accessReporter sends one TouchAccessTime per open of a file for reading
// (-accessReport). An open answered from the metadata cache never reaches the
// filer, so without the report a filer that tracks access cannot tell a cached
// file that is still read from one that is not. Reports are sent in the
// background and never hold up the open: a report that does not fit in the
// queue is dropped and counted.
type accessReporter struct {
	touch       func(ctx context.Context, path util.FullPath, atimeNs int64) error
	queue       chan accessReport
	ctx         context.Context // done once stop is called
	cancel      context.CancelFunc
	pausedUntil atomic.Int64 // unix nanoseconds
	dropped     atomic.Int64
	failed      atomic.Int64
}

// accessReport is one open for reading waiting to be sent.
type accessReport struct {
	path    util.FullPath
	atimeNs int64
}

// newAccessReporter returns a reporter that queues reports; start sends them.
func newAccessReporter(touch func(ctx context.Context, path util.FullPath, atimeNs int64) error) *accessReporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &accessReporter{
		touch:  touch,
		queue:  make(chan accessReport, accessReportQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

// start starts the workers that send the queued reports.
func (r *accessReporter) start() {
	for i := 0; i < accessReportWorkers; i++ {
		go r.work()
	}
	go r.logLosses()
}

// stop stops the workers and cancels the reports being sent. Reports made
// after stop are dropped.
func (r *accessReporter) stop() {
	r.cancel()
}

// report queues one read of path. It does nothing while reporting is paused.
func (r *accessReporter) report(path util.FullPath) {
	if r.paused() || r.ctx.Err() != nil {
		return
	}
	select {
	case r.queue <- accessReport{path: path, atimeNs: time.Now().UnixNano()}:
	default:
		r.dropped.Add(1)
	}
}

func (r *accessReporter) paused() bool {
	return time.Now().UnixNano() < r.pausedUntil.Load()
}

// work sends queued reports until stop. Reports still queued when reporting
// pauses are dropped: the filer that answered Unimplemented would answer
// them the same way.
func (r *accessReporter) work() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case report, ok := <-r.queue:
			if !ok {
				return
			}
			if !r.paused() {
				r.send(report)
			}
		}
	}
}

func (r *accessReporter) send(report accessReport) {
	ctx, cancel := context.WithTimeout(r.ctx, accessReportTimeout)
	defer cancel()
	err := r.touch(ctx, report.path, report.atimeNs)
	switch {
	case err == nil:
	case status.Code(err) == codes.Unimplemented:
		now := time.Now()
		if r.pausedUntil.Swap(now.Add(accessReportPause).UnixNano()) <= now.UnixNano() {
			glog.Warningf("access report: filer does not implement TouchAccessTime, pausing reports for %v", accessReportPause)
		}
	case r.ctx.Err() != nil:
		// stopped
	default:
		r.failed.Add(1)
		glog.V(1).Infof("access report %s: %v", report.path, err)
	}
}

func (r *accessReporter) logLosses() {
	ticker := time.NewTicker(accessReportLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}
		dropped, failed := r.dropped.Swap(0), r.failed.Swap(0)
		if dropped > 0 || failed > 0 {
			glog.Warningf("access report: %d opens not reported in the last %v (%d dropped on a full queue, %d failed)",
				dropped+failed, accessReportLogInterval, dropped, failed)
		}
	}
}

// reportOpen reports an open of a file for reading. A write-only open reads
// nothing and is not reported.
func (wfs *WFS) reportOpen(flags uint32, fh *FileHandle) {
	if wfs.accessReporter == nil || openFlagsToAccessMask(flags)&fuse.R_OK == 0 {
		return
	}
	if path := fh.FullPath(); path != "" {
		wfs.accessReporter.report(path)
	}
}

// touchAccessTime sends one report to the filer this mount uses or, if that
// filer cannot be reached in time, to the next one, which the mount then
// keeps using unless another call moved it meanwhile. Any answer,
// Unimplemented included, ends the walk.
func (wfs *WFS) touchAccessTime(ctx context.Context, path util.FullPath, atimeNs int64) error {
	dir, name := path.DirAndName()
	req := &filer_pb.TouchAccessTimeRequest{
		Directory:     dir,
		Name:          name,
		ClientAtimeNs: atimeNs,
	}
	maxWait := wfs.filerRPCWait()
	if maxWait > accessReportAttemptTimeout {
		maxWait = accessReportAttemptTimeout
	}
	addresses := wfs.option.FilerAddresses
	start := int(atomic.LoadInt32(&wfs.option.filerIndex))
	err := fmt.Errorf("no filer to report %s to", path)
	for x := 0; x < len(addresses); x++ {
		i := (start + x) % len(addresses)
		attemptWait := maxWait
		if deadline, ok := ctx.Deadline(); ok {
			// leave every filer not yet tried an equal share of the report
			if share := time.Until(deadline) / time.Duration(len(addresses)-x); share < attemptWait {
				attemptWait = share
			}
		}
		err = wfs.callFiler(ctx, false, attemptWait, addresses[i].ToGrpcAddress(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			_, err := client.TouchAccessTime(ctx, req)
			return err
		})
		if err == nil {
			atomic.CompareAndSwapInt32(&wfs.option.filerIndex, int32(start), int32(i))
			return nil
		}
		if !filerUnreachable(err) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// filerUnreachable reports whether err means the filer did not answer. A
// status the filer sent is its answer, whatever words its message contains.
func filerUnreachable(err error) bool {
	st, ok := util.ServerStatus(err)
	if !ok {
		return util.IsTransientError(err)
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	case codes.Canceled:
		return util.IsTransientError(err)
	}
	return false
}
