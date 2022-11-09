package filer_pb

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TraverseBfs(filerClient FilerClient, parentPath util.FullPath, fn func(parentPath util.FullPath, entry *Entry)) (err error) {
	K := 5

	var jobQueueWg sync.WaitGroup
	queue := util.NewQueue()
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
					if t == nil {
						time.Sleep(329 * time.Millisecond)
						continue
					}
					dir := t.(util.FullPath)
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

func processOneDirectory(filerClient FilerClient, parentPath util.FullPath, queue *util.Queue, jobQueueWg *sync.WaitGroup, fn func(parentPath util.FullPath, entry *Entry)) (err error) {

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
