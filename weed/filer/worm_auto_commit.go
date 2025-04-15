package filer

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/queue"
	"github.com/seaweedfs/seaweedfs/weed/util/workqueue"
	"k8s.io/utils/clock"
)

func (f *Filer) maybeCommitAsWORM(entry *Entry) {
	if f.wormAutoCommitController == nil || entry.WORMEnforcedAtTsNs > 0 {
		return
	}

	f.wormAutoCommitController.AddItem(entryItem{fullPath: entry.FullPath, mtime: entry.Mtime})
}

func (f *Filer) StartWormAutoCommitControllerInBackground(ctx context.Context) {
	controller := newWormAutoCommitController(f, nil)
	go func() {
		err := controller.loopAllWORMPaths(ctx)
		if err != nil {
			// restart the server to try again
			glog.Fatalf("Failed to loop all WORM paths: %v", err)

		}
	}()

	f.wormAutoCommitController = controller
	go controller.Run(ctx, 3)
}

type entryItem struct {
	fullPath util.FullPath
	mtime    time.Time
}

func (i entryItem) Compare(other queue.Item) int {
	o := other.(entryItem)
	// if i is before o, it should be processed first
	return i.mtime.Compare(o.mtime) * -1
}

func (i entryItem) GetUniqueID() string {
	return string(i.fullPath)
}

var _ queue.Item = entryItem{}

type wormAutoCommitController struct {
	queue workqueue.DelayingInterface
	clock clock.WithTicker
	filer *Filer
}

func newWormAutoCommitController(filer *Filer, ck clock.WithTicker) *wormAutoCommitController {
	if ck == nil {
		ck = clock.RealClock{}
	}

	return &wormAutoCommitController{
		queue: workqueue.NewDelayingQueueWithConfig(workqueue.DelayingQueueConfig{Clock: ck}),
		clock: ck,
		filer: filer,
	}
}

func (s *wormAutoCommitController) AddItem(item queue.Item) {
	s.queue.Add(item)
}

func (s *wormAutoCommitController) loopAllWORMPaths(ctx context.Context) error {
	paths := s.filer.FilerConf.GetWORMPaths()
	for _, path := range paths {
		if err := s.loopPath(ctx, util.FullPath(path)); err != nil {
			return err
		}
	}

	return nil
}

func (s *wormAutoCommitController) loopPath(ctx context.Context, path util.FullPath) error {
	lastFileName := ""
	for {
		entries, hasMore, err := s.filer.ListDirectoryEntries(ctx, path, lastFileName, false, 1024, "", "", "")
		if err != nil {
			return err
		}

		for _, entry := range entries {
			lastFileName = entry.Name()
			if entry.IsDirectory() {
				err = s.loopPath(ctx, entry.FullPath)
				if err != nil {
					return err
				}
			} else {
				if entry.WORMEnforcedAtTsNs == 0 {
					s.AddItem(entryItem{fullPath: entry.FullPath, mtime: entry.Mtime})
				}
			}
		}
		if !hasMore {
			break
		}
	}

	return nil
}

func (s *wormAutoCommitController) Run(ctx context.Context, workers int) {
	defer s.queue.ShutDown()

	glog.Infof("Starting wormAutoCommitController, workers: %d", workers)
	defer glog.Info("Stopping wormAutoCommitController")

	for i := 0; i < workers; i++ {
		go s.worker(ctx)
	}

	<-ctx.Done()
}

func (s *wormAutoCommitController) worker(ctx context.Context) {
	for s.processNextWorkItem(ctx) {
	}
}

func (s *wormAutoCommitController) processNextWorkItem(ctx context.Context) bool {
	key, closed := s.queue.Get()
	if closed {
		return false
	}
	defer s.queue.Done(key)

	err := s.process(ctx, key.(entryItem).fullPath)
	if err != nil {
		glog.Errorf("Failed to process %s, try it again: %v", key, err)
		s.queue.AddAfter(key, 100*time.Millisecond)
	}

	return true
}

func (s *wormAutoCommitController) process(ctx context.Context, fullPath util.FullPath) error {
	yes, entry, err := s.enforceNow(ctx, fullPath)
	if err != nil {
		return err
	} else if !yes {
		return nil
	}

	glog.Infof("the time elapsed since the entry's mtime is more than its grace period, WORM is enforced, entry: %s, mtime: %s", fullPath, entry.Mtime)
	entry.WORMEnforcedAtTsNs = s.clock.Now().UnixNano()

	// update the entry
	return s.filer.CreateEntry(ctx, entry, false, false, nil, false, s.filer.MaxFilenameLength)
}

func (s *wormAutoCommitController) enforceNow(ctx context.Context, fullPath util.FullPath) (bool, *Entry, error) {
	rule := s.filer.FilerConf.MatchStorageRule(string(fullPath))
	if !rule.Worm {
		return false, nil, nil
	}

	entry, err := s.filer.FindEntry(ctx, fullPath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil, nil
		}

		return false, nil, err
	}

	// grace period is already set
	if entry.WORMEnforcedAtTsNs > 0 {
		return false, nil, nil
	}

	silentTime := s.clock.Now().Sub(entry.Mtime)
	if silentTime >= time.Duration(rule.WormGracePeriodSeconds)*time.Second {
		return true, entry, nil
	}

	s.queue.AddAfter(entryItem{mtime: entry.Mtime, fullPath: fullPath}, time.Duration(rule.WormGracePeriodSeconds)*time.Second-silentTime)

	return false, nil, nil
}
