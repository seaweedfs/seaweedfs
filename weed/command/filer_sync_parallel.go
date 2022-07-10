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

type WorkerEvents []*filer_pb.SubscribeMetadataResponse
type WorkerEventsGroup []WorkerEvents
type EventIndexes []int

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
	name          string
	eventsIndexes EventIndexes
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
			if eventsSize >= cache.parallelBatchSize || cache.lastSyncTsNs > 0 && time.Since(time.Unix(0, cache.lastSyncTsNs)) >= cache.parallelWaitTime {
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
	rootNode := buildEventTree(cache.events)
	eventIndexesGroup := buildEventIndexesGroup(rootNode)
	eventGroup := buildWorkerEventsGroup(cache.parallelNum, eventIndexesGroup, cache.events)
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

func buildEventTree(originalEvents []*filer_pb.SubscribeMetadataResponse) ParallelSyncNode {
	// build a tree. record event index
	rootNode := createNode([]string{"/"})
	for i, event := range originalEvents {
		rootNode.addNode(i, getEventAffectedFullPath(event))
	}
	return rootNode
}

func getEventAffectedFullPath(event *filer_pb.SubscribeMetadataResponse) []string {
	var sourceOldKey, sourceNewKey util.FullPath
	if event.EventNotification.OldEntry != nil {
		sourceOldKey = util.FullPath(event.Directory).Child(event.EventNotification.OldEntry.Name)
	}
	if event.EventNotification.NewEntry != nil {
		sourceNewKey = util.FullPath(event.EventNotification.NewParentPath).Child(event.EventNotification.NewEntry.Name)
	}
	affectedPath := sourceOldKey
	if sourceOldKey == "" {
		affectedPath = sourceNewKey
	}
	if strings.HasSuffix(string(affectedPath), "/") {
		affectedPath = affectedPath[:len(affectedPath)-1]
	}
	pathSplit := affectedPath.Split()
	return pathSplit
}

func buildWorkerEventsGroup(parallelNum int, eventIndexesGroup []EventIndexes, originalEvents []*filer_pb.SubscribeMetadataResponse) WorkerEventsGroup {
	workerEventsGroup := make(WorkerEventsGroup, parallelNum)
	if eventIndexesGroup == nil {
		return workerEventsGroup
	}
	for _, eventIndexes := range eventIndexesGroup {
		events := getEventsByIndexes(eventIndexes, originalEvents)
		// distribute events evenly
		putEventsIntoMinLengthWorkerEvents(workerEventsGroup, events)
	}
	return workerEventsGroup
}

func getEventsByIndexes(eventIndexes EventIndexes, originalEvents []*filer_pb.SubscribeMetadataResponse) []*filer_pb.SubscribeMetadataResponse {
	var workerEvents WorkerEvents
	for _, index := range eventIndexes {
		workerEvents = append(workerEvents, originalEvents[index])
	}
	return workerEvents
}

func putEventsIntoMinLengthWorkerEvents(workerEventsGroup WorkerEventsGroup, eventGroup []*filer_pb.SubscribeMetadataResponse) {
	minLengthWorkerIndex := getWorkerEventsGroupMinLengthIndex(workerEventsGroup)
	workerEventsGroup[minLengthWorkerIndex] = append(workerEventsGroup[minLengthWorkerIndex], eventGroup...)
}

func getWorkerEventsGroupMinLengthIndex(workerEventsGroup WorkerEventsGroup) int {
	minLengthIndex := 0
	minLength := len(workerEventsGroup[0])
	for i := 0; i < len(workerEventsGroup); i++ {
		length := len(workerEventsGroup[i])
		if length < minLength {
			minLengthIndex = i
			minLength = length
		}
	}
	return minLengthIndex
}

func buildEventIndexesGroup(node ParallelSyncNode) []EventIndexes {
	var eventIndexesGroup []EventIndexes
	generateEventIndexesGroupByNode(node, &eventIndexesGroup)
	return eventIndexesGroup
}

func generateEventIndexesGroupByNode(node ParallelSyncNode, eventIndexesGroup *[]EventIndexes) {
	if len(node.eventsIndexes) > 0 {
		nodeEventIndexes := node.getEventIndexes()
		if len(nodeEventIndexes) > 0 {
			sort.Ints(nodeEventIndexes)
			*eventIndexesGroup = append(*eventIndexesGroup, nodeEventIndexes)
		}
	} else {
		if node.hasChild() {
			for _, child := range node.child {
				generateEventIndexesGroupByNode(*child, eventIndexesGroup)
			}
		}
	}
}

func createNode(path []string) ParallelSyncNode {
	var currentPathName string
	if path[0] == "/" {
		return ParallelSyncNode{
			name:     "",
			fullPath: []string{},
		}
	}
	// Remove the tail slash
	if len(path) > 1 && path[len(path)-1] == "" {
		path = path[:len(path)-1]
	}
	if len(path) > 0 {
		currentPathName = path[len(path)-1]
	}
	node := ParallelSyncNode{
		name:     currentPathName,
		fullPath: path,
	}
	return node
}

func generateEventIndexes(node ParallelSyncNode, indexes *EventIndexes) {
	for _, eventIndex := range node.eventsIndexes {
		if eventIndex >= 0 {
			*indexes = append(*indexes, eventIndex)
		}
	}
	if node.hasChild() {
		for _, child := range node.child {
			generateEventIndexes(*child, indexes)
		}
	}
}

func (p *ParallelSyncNode) getEventIndexes() EventIndexes {
	var indexes EventIndexes
	generateEventIndexes(*p, &indexes)
	return indexes
}

func (p *ParallelSyncNode) addNode(currentIdx int, curDealPathArray []string) (bool, *ParallelSyncNode) {
	if len(curDealPathArray) > 0 {
		curDealPathName := curDealPathArray[0]
		// check if child nodes have the same path
		for _, child := range p.child {
			if child.name == curDealPathName {
				if len(curDealPathArray) > 1 {
					return child.addNode(currentIdx, curDealPathArray[1:])
				} else {
					child.eventsIndexes = append(child.eventsIndexes, currentIdx)
					return true, child
				}
			}
		}

		idx := -1
		if len(curDealPathArray) == 1 {
			idx = currentIdx
		}

		createNodeFullPath := append(p.fullPath, curDealPathName)
		newNode := createNode(createNodeFullPath)
		p.addChild(&newNode)
		if len(curDealPathArray) > 1 {
			return newNode.addNode(currentIdx, curDealPathArray[1:])
		}
		// record event subscript
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
