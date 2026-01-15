package filer_pb

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TraverseBfs(ctx context.Context, filerClient FilerClient, parentPath util.FullPath, fn func(parentPath util.FullPath, entry *Entry) error) (err error) {
	K := 5

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queue := util.NewQueue[util.FullPath]()
	var pending sync.WaitGroup
	pending.Add(1)
	queue.Enqueue(parentPath)

	var hasError int32
	var firstErr error

	enqueue := func(p util.FullPath) bool {
		// Stop expanding traversal once canceled (e.g. first error encountered).
		if ctx.Err() != nil {
			return false
		}
		pending.Add(1)
		queue.Enqueue(p)
		return true
	}

	done := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < K; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				dir := queue.Dequeue()
				if dir == "" {
					// queue is empty for now
					select {
					case <-done:
						return
					case <-time.After(50 * time.Millisecond):
						continue
					}
				}

				// Always mark the directory as done so the closer can finish.
				if ctx.Err() == nil {
					processErr := processOneDirectory(ctx, filerClient, dir, enqueue, fn)
					if processErr != nil {
						if atomic.CompareAndSwapInt32(&hasError, 0, 1) {
							firstErr = processErr
							cancel()
						}
					}
				}
				pending.Done()
			}
		}()
	}

	pending.Wait()
	close(done)

	workers.Wait()

	return firstErr
}

func processOneDirectory(ctx context.Context, filerClient FilerClient, parentPath util.FullPath, enqueue func(p util.FullPath) bool, fn func(parentPath util.FullPath, entry *Entry) error) (err error) {

	return ReadDirAllEntries(ctx, filerClient, parentPath, "", func(entry *Entry, isLast bool) error {

		if err := fn(parentPath, entry); err != nil {
			return err
		}

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
			if parentPath == "/" {
				subDir = "/" + entry.Name
			}
			enqueue(util.FullPath(subDir))
		}
		return nil
	})

}

func StreamBfs(client SeaweedFilerClient, dir util.FullPath, olderThanTsNs int64, fn func(parentPath util.FullPath, entry *Entry) error) (err error) {
	glog.V(0).Infof("TraverseBfsMetadata %v if before %v", dir, time.Unix(0, olderThanTsNs))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.TraverseBfsMetadata(ctx, &TraverseBfsMetadataRequest{
		Directory: string(dir),
	})
	if err != nil {
		return fmt.Errorf("traverse bfs metadata: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("traverse bfs metadata: %w", err)
		}
		if err := fn(util.FullPath(resp.Directory), resp.Entry); err != nil {
			return err
		}
	}
	return nil
}
