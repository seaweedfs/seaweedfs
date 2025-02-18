package filer_pb

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TraverseBfs(filerClient FilerClient, parentPath util.FullPath, fn func(parentPath util.FullPath, entry *Entry)) (err error) {
	K := 5

	var jobQueueWg sync.WaitGroup
	queue := util.NewQueue[util.FullPath]()
	jobQueueWg.Add(1)
	queue.Enqueue(parentPath)
	terminates := make([]chan bool, K)

	for i := 0; i < K; i++ {
		terminates[i] = make(chan bool)
		go func(j int) {
			for {
				select {
				case <-terminates[j]:
					return
				default:
					t := queue.Dequeue()
					if t == "" {
						time.Sleep(329 * time.Millisecond)
						continue
					}
					dir := t
					processErr := processOneDirectory(filerClient, dir, queue, &jobQueueWg, fn)
					if processErr != nil {
						err = processErr
					}
					jobQueueWg.Done()
				}
			}
		}(i)
	}
	jobQueueWg.Wait()
	for i := 0; i < K; i++ {
		close(terminates[i])
	}
	return
}

func processOneDirectory(filerClient FilerClient, parentPath util.FullPath, queue *util.Queue[util.FullPath], jobQueueWg *sync.WaitGroup, fn func(parentPath util.FullPath, entry *Entry)) (err error) {

	return ReadDirAllEntries(filerClient, parentPath, "", func(entry *Entry, isLast bool) error {

		fn(parentPath, entry)

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
			if parentPath == "/" {
				subDir = "/" + entry.Name
			}
			jobQueueWg.Add(1)
			queue.Enqueue(util.FullPath(subDir))
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
		return fmt.Errorf("traverse bfs metadata: %v", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("traverse bfs metadata: %v", err)
		}
		if err := fn(util.FullPath(resp.Directory), resp.Entry); err != nil {
			return err
		}
	}
	return nil
}
