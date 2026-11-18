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
	accessReportTimeout   = 10 * time.Second
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
	pausedUntil atomic.Int64 // unix nanoseconds
	dropped     atomic.Int64
	failed      atomic.Int64
}

// accessReport is one open for reading waiting to be sent.
type accessReport struct {
	path    util.FullPath
	atimeNs int64
}

func newAccessReporter(touch func(ctx context.Context, path util.FullPath, atimeNs int64) error) *accessReporter {
	r := &accessReporter{
		touch: touch,
		queue: make(chan accessReport, accessReportQueueSize),
	}
	for i := 0; i < accessReportWorkers; i++ {
		go r.work()
	}
	go r.logLosses()
	return r
}

// report queues one read of path. It does nothing while reporting is paused.
func (r *accessReporter) report(path util.FullPath) {
	if r.paused() {
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

func (r *accessReporter) work() {
	for report := range r.queue {
		r.send(report)
	}
}

func (r *accessReporter) send(report accessReport) {
	ctx, cancel := context.WithTimeout(context.Background(), accessReportTimeout)
	defer cancel()
	err := r.touch(ctx, report.path, report.atimeNs)
	switch {
	case err == nil:
	case status.Code(err) == codes.Unimplemented:
		now := time.Now()
		if r.pausedUntil.Swap(now.Add(accessReportPause).UnixNano()) <= now.UnixNano() {
			glog.Warningf("access report: filer does not implement TouchAccessTime, pausing reports for %v", accessReportPause)
		}
	default:
		r.failed.Add(1)
		glog.V(1).Infof("access report %s: %v", report.path, err)
	}
}

func (r *accessReporter) logLosses() {
	ticker := time.NewTicker(accessReportLogInterval)
	defer ticker.Stop()
	for range ticker.C {
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
// filer cannot be reached, to the next one. An Unimplemented answer is
// returned at once, so the reporter pauses instead of trying every filer.
func (wfs *WFS) touchAccessTime(ctx context.Context, path util.FullPath, atimeNs int64) error {
	dir, name := path.DirAndName()
	req := &filer_pb.TouchAccessTimeRequest{
		Directory:     dir,
		Name:          name,
		ClientAtimeNs: atimeNs,
	}
	addresses := wfs.option.FilerAddresses
	start := int(atomic.LoadInt32(&wfs.option.filerIndex))
	var lastErr error
	for x := 0; x < len(addresses); x++ {
		address := addresses[(start+x)%len(addresses)].ToGrpcAddress()
		err := wfs.callFiler(ctx, false, wfs.filerRPCWait(), address, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			_, err := client.TouchAccessTime(ctx, req)
			return err
		})
		if err == nil || status.Code(err) == codes.Unimplemented || ctx.Err() != nil {
			return err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no filer to report %s to", path)
	}
	return lastErr
}
