package remote_storage

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"sync"
	"time"
)

type ListDirectoryFunc func(parentDir util.FullPath, visitFn VisitFunc) error

func TraverseBfs(listDirFn ListDirectoryFunc, parentPath util.FullPath, visitFn VisitFunc) (err error) {
	K := 5

	var dirQueueWg sync.WaitGroup
	dirQueue := util.NewQueue()
	dirQueueWg.Add(1)
	dirQueue.Enqueue(parentPath)
	var isTerminating bool

	for i := 0; i < K; i++ {
		go func() {
			for {
				if isTerminating {
					break
				}
				t := dirQueue.Dequeue()
				if t == nil {
					time.Sleep(329 * time.Millisecond)
					continue
				}
				dir := t.(util.FullPath)
				processErr := processOneDirectory(listDirFn, dir, visitFn, dirQueue, &dirQueueWg)
				if processErr != nil {
					err = processErr
				}
				dirQueueWg.Done()
			}
		}()
	}

	dirQueueWg.Wait()
	isTerminating = true
	return

}

func processOneDirectory(listDirFn ListDirectoryFunc, parentPath util.FullPath, visitFn VisitFunc, dirQueue *util.Queue, dirQueueWg *sync.WaitGroup) error {

	return listDirFn(parentPath, func(dir string, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		if err := visitFn(dir, name, isDirectory, remoteEntry); err != nil {
			return err
		}
		if !isDirectory {
			return nil
		}
		dirQueueWg.Add(1)
		dirQueue.Enqueue(parentPath.Child(name))
		return nil
	})

}
