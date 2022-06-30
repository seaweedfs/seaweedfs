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
	events            []*filer_pb.SubscribeMetadataResponse
	eventsChan        chan *filer_pb.SubscribeMetadataResponse
	parallelNum       int
	parallelBatchSize int
	parallelWaitTime  time.Duration
	lastSyncTsNs      int64
	lastTsNs          int64
	sourceFiler       pb.ServerAddress
	targetFiler       pb.ServerAddress
	persistEventFns   []func(resp *filer_pb.SubscribeMetadataResponse) error
	cancelChan        chan struct{}
}

type ParallelSyncNode struct {
	curPathName   string
	eventsIndexes []int
	fullPathName  string
	fullPath      []string
	child         []*ParallelSyncNode
}

func processEventWithOffsetFunc(cache *ParallelSyncMetadataCache, targetFilerSignature int32) pb.ProcessMetadataFunc {
	return pb.AddOffsetParallelSyncFunc(func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		for _, sig := range message.Signatures {
			if sig == targetFilerSignature && targetFilerSignature != 0 {
				fmt.Printf("%s skipping %s change to %v\n", cache.targetFiler, cache.sourceFiler, message)
				return nil
			}
		}
		cache.eventsChan <- resp
		return nil
	})
}

func startEventsConsumer(cache *ParallelSyncMetadataCache, setOffsetFunc func(counter int64, lastTsNs int64) error) {
	glog.Infof("start event synchronization consumer. from %s to %s", cache.sourceFiler, cache.targetFiler)
	for {
		select {
		case <-cache.cancelChan:
			close(cache.cancelChan)
			return
		case event := <-cache.eventsChan:
			cache.events = append(cache.events, event)
			cache.lastTsNs = event.TsNs
			eventsSize := len(cache.events)
			if eventsSize >= cache.parallelBatchSize || time.Since(time.Unix(0, cache.lastSyncTsNs)) >= cache.parallelWaitTime {
				persistEvents(cache, setOffsetFunc)
			}
		case <-time.After(cache.parallelWaitTime):
			eventsSize := len(cache.events)
			if eventsSize > 0 && cache.lastTsNs > 0 {
				persistEvents(cache, setOffsetFunc)
			}
		}
	}
}

func persistEvents(cache *ParallelSyncMetadataCache, setOffsetFn func(counter int64, lastTsNs int64) error) {
	cache.lastSyncTsNs = time.Now().UnixNano()
	syncLength := len(cache.events)
	if syncLength == 0 {
		return
	}

	// need to wait for all tasks to be processed
	wg := sync.WaitGroup{}
	wg.Add(cache.parallelNum)
	// assign events to each worker thread
	eventGroup := buildWorkerGroup(cache.parallelNum, cache.events)
	for i := 0; i < cache.parallelNum; i++ {
		go func(events []*filer_pb.SubscribeMetadataResponse, idx int, cache *ParallelSyncMetadataCache) {
			defer wg.Done()
			for _, eventMsg := range events {
				err := cache.persistEventFns[idx](eventMsg)
				if err != nil {
					util.RetryForever("syncEvent", func() error {
						return cache.persistEventFns[idx](eventMsg)
					}, func(err error) bool {
						glog.Errorf("sync event msg %v: %v", eventMsg, err)
						return true
					})
				}
			}
		}(eventGroup[i], i, cache)
	}
	wg.Wait()
	cache.events = cache.events[:0]

	util.RetryForever("syncEventOffset", func() error {
		return setOffsetFn(int64(syncLength), cache.lastTsNs)
	}, func(err error) bool {
		glog.Errorf("sync offset %v: %v", cache.lastTsNs, err)
		return true
	})
}

func buildWorkerGroup(parallelNum int, events []*filer_pb.SubscribeMetadataResponse) [][]*filer_pb.SubscribeMetadataResponse {
	// build a tree. record event index
	rootTree := ParallelSyncNode{fullPathName: "/", fullPath: []string{}, curPathName: ""}

	for i := 0; i < len(events); i++ {
		resp := events[i]
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

	return getEventGroupByNode(rootTree, parallelNum, events)
}

func getEventGroupByNode(rootTree ParallelSyncNode, parallelNum int, events []*filer_pb.SubscribeMetadataResponse) [][]*filer_pb.SubscribeMetadataResponse {
	var eventIndexesGroup [][]int
	getEventIndexesByNode(rootTree, &eventIndexesGroup)

	result := make([][]*filer_pb.SubscribeMetadataResponse, parallelNum)
	if eventIndexesGroup == nil {
		return result
	}
	for _, eventIndexes := range eventIndexesGroup {
		var eventGroup []*filer_pb.SubscribeMetadataResponse
		for _, index := range eventIndexes {
			eventGroup = append(eventGroup, events[index])
		}
		// distribute events evenly
		minLengthWorkerIndex := getWorkerGroupMinLengthIndex(result)
		result[minLengthWorkerIndex] = append(result[minLengthWorkerIndex], eventGroup...)
	}

	return result
}

func getWorkerGroupMinLengthIndex(workerGroup [][]*filer_pb.SubscribeMetadataResponse) int {
	minIndex := 0
	curWorkerGroupLength := len(workerGroup[0])
	for i := 0; i < len(workerGroup); i++ {
		length := len(workerGroup[i])
		if length < curWorkerGroupLength {
			minIndex = i
			curWorkerGroupLength = length
		}
	}
	return minIndex
}

func getEventIndexesByNode(node ParallelSyncNode, eventIndexes *[][]int) {
	if len(node.eventsIndexes) > 0 {
		var nodeEventIndexes []int
		node.getEventIndexes(&nodeEventIndexes)
		if len(nodeEventIndexes) > 0 {
			sort.Ints(nodeEventIndexes)
			*eventIndexes = append(*eventIndexes, nodeEventIndexes)
		}
	} else {
		if node.hasChild() {
			for _, child := range node.child {
				getEventIndexesByNode(*child, eventIndexes)
			}
		}
	}
}

func (p *ParallelSyncNode) getEventIndexes(indexes *[]int) {
	for _, eventIndex := range p.eventsIndexes {
		if eventIndex >= 0 {
			*indexes = append(*indexes, eventIndex)
		}
	}
	if p.hasChild() {
		for _, child := range p.child {
			child.getEventIndexes(indexes)
		}
	}
}

func (p *ParallelSyncNode) addNode(curIdx int, curNodePathArray []string) (bool, *ParallelSyncNode) {
	if len(curNodePathArray) > 0 {
		curPathName := curNodePathArray[0]
		curFullPathName := "/" + curPathName
		if p.fullPathName != "/" {
			curFullPathName = p.fullPathName + "/" + curPathName
		}
		// check if child nodes have the same path
		for _, child := range p.child {
			if child.curPathName == curPathName {
				if len(curNodePathArray) > 1 {
					return child.addNode(curIdx, curNodePathArray[1:])
				} else {
					child.eventsIndexes = append(child.eventsIndexes, curIdx)
					return true, child
				}
			}
		}

		fullPath := strings.Split(curFullPathName, "/")[1:]
		idx := -1
		if len(curNodePathArray) == 1 {
			idx = curIdx
		}

		newNode := ParallelSyncNode{
			curPathName:   curPathName,
			eventsIndexes: []int{},
			fullPathName:  curFullPathName,
			fullPath:      fullPath,
			child:         []*ParallelSyncNode{},
		}
		p.addChild(&newNode)
		if len(curNodePathArray) > 1 {
			return newNode.addNode(curIdx, curNodePathArray[1:])
		}
		newNode.eventsIndexes = append(newNode.eventsIndexes, idx)
		return false, &newNode
	}
	return false, nil
}

func (p *ParallelSyncNode) hasChild() bool {
	return len(p.child) > 0
}

func (p *ParallelSyncNode) addChild(child *ParallelSyncNode) {
	p.child = append(p.child, child)
}
