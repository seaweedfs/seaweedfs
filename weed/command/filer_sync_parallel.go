package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"sort"
	"strings"
	"sync"
	"time"
)

type ParallelSyncMetadataCache struct {
	lock              sync.RWMutex
	l                 []*filer_pb.SubscribeMetadataResponse
	ParallelNum       int
	ParallelBatchSize int64
	lastTime          int64
	TsNs              int64
	sourceFiler       string
	targetFiler       string
	persistEventFns   []func(resp *filer_pb.SubscribeMetadataResponse) error
}

type ParallelSyncNode struct {
	key      string
	value    []int
	path     string
	fullPath []string
	child    []*ParallelSyncNode
}

func getProcessEventFnWithOffsetFunc(c *ParallelSyncMetadataCache, sourceFiler pb.ServerAddress, targetFiler pb.ServerAddress, targetFilerSignature int32,
	setOffsetFunc func(counter int64, lastTsNs int64) error) pb.ProcessMetadataFunc {
	return pb.AddOffsetParallelSyncFunc(func(resp *filer_pb.SubscribeMetadataResponse) (error, int64) {
		message := resp.EventNotification
		for _, sig := range message.Signatures {
			if sig == targetFilerSignature && targetFilerSignature != 0 {
				fmt.Printf("%s skipping %s change to %v\n", targetFiler, sourceFiler, message)
				return nil, 1
			}
		}
		return persistEventParallelSyncFunc(c, resp)
	}, 3*time.Second, setOffsetFunc)
}

var runParallelSyncScheduledTask = func(cache *ParallelSyncMetadataCache, stop chan struct{}, d time.Duration, setOffsetFunc func(counter int64, lastTsNs int64) error) {
	go func(c *ParallelSyncMetadataCache) {
		for range time.Tick(d) {
			select {
			case <-stop:
				close(stop)
				return
			default:
				if len(c.l) > 0 && time.Since(time.UnixMilli(c.lastTime)) >= d {
					counter := doPersistEventParallel(c)
					// it's not necessary to set offset successfully. Just wait until next time.
					_ = setOffsetFunc(counter, c.TsNs)
				}
			}
		}
	}(cache)
}

func persistEventParallelSyncFunc(c *ParallelSyncMetadataCache, resp *filer_pb.SubscribeMetadataResponse) (error, int64) {
	// Scheduled tasks may trigger empty operation, which needs to be locked
	c.lock.Lock()
	c.l = append(c.l, resp)
	c.TsNs = resp.TsNs
	sendSize := int64(len(c.l))
	c.lock.Unlock()
	if sendSize < c.ParallelBatchSize {
		return nil, 0
	}
	doPersistEventParallel(c)
	return nil, sendSize
}

// doPersistEventParallel For parallel persistence of metadata
func doPersistEventParallel(c *ParallelSyncMetadataCache) int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lastTime = time.Now().UnixMilli()
	var finishTime = time.Now()
	syncLength := len(c.l)
	if syncLength == 0 {
		return 0
	}

	// need to wait for all the split results to be processed
	wg := sync.WaitGroup{}
	wg.Add(c.ParallelNum)
	// assign tasks to each worker thread
	sendGroup := splitMetadataResponseByPath(c.ParallelNum, c.l)
	for i := 0; i < c.ParallelNum; i++ {
		go func(data []*filer_pb.SubscribeMetadataResponse, idx int, c *ParallelSyncMetadataCache) {
			defer wg.Done()
			for _, eventMsg := range data {
				err := c.persistEventFns[idx](eventMsg)
				if err != nil {
					util.RetryForever("parallelSyncMeta", func() error {
						return c.persistEventFns[idx](eventMsg)
					}, func(err error) bool {
						glog.Errorf("parallel sync process %v: %v", eventMsg, err)
						return true
					})
				}
			}
		}(sendGroup[i], i, c)
	}
	wg.Wait()
	c.l = c.l[:0]

	glog.Infof("parallel sync %s to %s, cost: %dms, worker:%d send size:%d", c.sourceFiler, c.targetFiler, time.Since(finishTime).Milliseconds(), c.ParallelNum, syncLength)

	return int64(syncLength)
}

// splitMetadataResponseByPath Split MetadataResponse to array. The result is not affect the processing order.
func splitMetadataResponseByPath(parallelNum int, list []*filer_pb.SubscribeMetadataResponse) [][]*filer_pb.SubscribeMetadataResponse {
	// build a tree. the value is the affected index of list.
	rootTree := ParallelSyncNode{path: "/", fullPath: []string{}, key: ""}

	for i := 0; i < len(list); i++ {
		resp := list[i]
		var sourceOldKey, sourceNewKey util.FullPath
		if resp.EventNotification.OldEntry != nil {
			sourceOldKey = util.FullPath(resp.Directory).Child(resp.EventNotification.OldEntry.Name)
		}
		if resp.EventNotification.NewEntry != nil {
			sourceNewKey = util.FullPath(resp.EventNotification.NewParentPath).Child(resp.EventNotification.NewEntry.Name)
		}

		affectedPath := sourceOldKey
		if sourceOldKey == "" {
			affectedPath = sourceNewKey
		}
		rootTree.addNode(i, affectedPath.Split())
	}

	return getParallelSyncWorkerArray(rootTree, parallelNum, list)
}

// Assign tasks to each worker's array
func getParallelSyncWorkerArray(rootTree ParallelSyncNode, parallelNum int, list []*filer_pb.SubscribeMetadataResponse) [][]*filer_pb.SubscribeMetadataResponse {
	var workerGroupResultArray [][]int
	getParallelSyncIndexByNode(rootTree, &workerGroupResultArray)

	result := make([][]*filer_pb.SubscribeMetadataResponse, parallelNum)
	if workerGroupResultArray == nil {
		return result
	}
	for _, d := range workerGroupResultArray {
		var itemGroup []*filer_pb.SubscribeMetadataResponse
		for _, i := range d {
			itemGroup = append(itemGroup, list[i])
		}
		// distribute tasks as evenly as possible
		mIdx := getMinLenIdxFromWorkerGroup(result)
		result[mIdx] = append(result[mIdx], itemGroup...)
	}

	return result
}

func getMinLenIdxFromWorkerGroup(workerGroup [][]*filer_pb.SubscribeMetadataResponse) int {
	mIdx := 0
	m := len(workerGroup[0])
	for i := 0; i < len(workerGroup); i++ {
		l := len(workerGroup[i])
		if l < m {
			mIdx = i
			m = l
		}
	}
	return mIdx
}

func getParallelSyncIndexByNode(node ParallelSyncNode, splitResult *[][]int) {
	if len(node.value) > 0 {
		// a litter of value
		var aLitterOfValue []int
		node.getAllValue(&aLitterOfValue)
		if len(aLitterOfValue) > 0 {
			sort.Ints(aLitterOfValue)
			*splitResult = append(*splitResult, aLitterOfValue)
		}
	} else {
		if node.hasChild() {
			for _, child := range node.child {
				getParallelSyncIndexByNode(*child, splitResult)
			}
		}
	}
}

// get all child value of node
func (p *ParallelSyncNode) getAllValue(values *[]int) {
	for _, d := range p.value {
		if d > -1 {
			*values = append(*values, d)
		}
	}
	if p.hasChild() {
		for _, child := range p.child {
			child.getAllValue(values)
		}
	}
}

func (p *ParallelSyncNode) addNode(curIdx int, curNodePathArray []string) (bool, *ParallelSyncNode) {
	if len(curNodePathArray) > 0 {
		curPath := curNodePathArray[0]
		curFullPathName := "/" + curPath
		if p.path != "/" {
			curFullPathName = p.path + "/" + curPath
		}

		for _, child := range p.child {
			if child.key == curPath {
				if len(curNodePathArray) > 1 {
					return child.addNode(curIdx, curNodePathArray[1:])
				} else {
					child.value = append(child.value, curIdx)
					return true, child
				}
			}
		}
		// add new node to p.child
		fP := strings.Split(curFullPathName, "/")[1:]
		idx := -1
		if len(curNodePathArray) == 1 {
			idx = curIdx
		}

		newNode := ParallelSyncNode{
			key:      curPath,
			value:    []int{},
			path:     curFullPathName,
			fullPath: fP,
			child:    []*ParallelSyncNode{},
		}
		p.child = append(p.child, &newNode)
		// it's not over. need to continue to add children to newNode
		if len(curNodePathArray) > 1 {
			return newNode.addNode(curIdx, curNodePathArray[1:])
		} else {
			newNode.value = append(newNode.value, idx)
			return false, &newNode
		}
	} else {
		return false, nil
	}
}

func (p *ParallelSyncNode) hasChild() bool {
	return len(p.child) > 0
}

func (p *ParallelSyncNode) addChild(child *ParallelSyncNode) {
	p.child = append(p.child, child)
}
