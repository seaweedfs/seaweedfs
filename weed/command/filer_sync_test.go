// Package
// @Author quzhihao
// @Date 2022/6/23
package command

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// test add files
// ADD ACTION: add 100 folders and Each folder contains 100 1kb files
//  sync cost: 190s
// async cost: 165s [parallelNum: 10, parallelBatchSize: 500, parallelWaitTime:15s]
// async cost: 140s [parallelNum: 10, parallelBatchSize: 1000, parallelWaitTime:10s]
// async cost: 140s [parallelNum: 20, parallelBatchSize: 1000, parallelWaitTime:20s]
func TestParallelSyncBatchAddFiles(t *testing.T) {
	t.SkipNow()
	fileFolderNumber := 100
	fileNumber := 100
	buffers, _ := getFile1KBBytes()
	filerUrl := "http://localhost:8888/test"
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
	startTime := time.Now()

	for i := 0; i < fileFolderNumber; i++ {
		for j := 0; j < fileNumber; j++ {
			address := filerUrl + "/" + strconv.Itoa(i) + "/" + strconv.Itoa(j)
			createFile1KB(client, address, buffers)
		}
	}
	printCostTime(startTime)
}

// test delete files
// DELETE ACTION: Recursive delete 100 folder and Each folder contains 100 1kb files
//  sync cost: 88s
// async cost: 75s [parallelNum: 10, parallelBatchSize: 500, parallelWaitTime:15s]
// async cost: 65s [parallelNum: 10, parallelBatchSize: 1000, parallelWaitTime:20s]
// async cost: 61s [parallelNum: 20, parallelBatchSize: 1000, parallelWaitTime:20s]
func TestParallelSyncBatchDeleteFiles(t *testing.T) {
	t.SkipNow()
	// Can be tested in linkage with TestParallelSyncBatchAddFiles
	fileFolderNumber := 100
	fileNumber := 100
	filerUrl := "http://localhost:8888/test"

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
	startTime := time.Now()
	// delete all files
	for i := 0; i < fileFolderNumber; i++ {
		folderAddress := filerUrl + "/" + strconv.Itoa(i)
		for j := 0; j < fileNumber; j++ {
			address := folderAddress + "/" + strconv.Itoa(j)
			deleteFilesOrFolders(client, address)
		}
		// delete all folders
		deleteFilesOrFolders(client, folderAddress)
	}
	printCostTime(startTime)
}

func TestParallelSyncHybrid(t *testing.T) {
	t.SkipNow()
	deleteFolder := false
	rootPath := "/test"
	aFilerUrl := "http://localhost:8888" + rootPath
	buffers, _ := getFile1KBBytes()
	var renameList []string

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
	startTime := time.Now()

	data := [][]map[string]string{
		0: {
			{"action": "addChild", "value": "1,2,3,4,5"},
			{"action": "deleteChild", "value": "1,2"},
		},
		1: {
			{"action": "addChild", "value": "6,7,8,9,10"},
			{"action": "move", "value": "6,7,8,9,10:/2"},
			{"action": "delete", "value": ""},
		},
		2: {
			{"action": "addChild", "value": "1,2,3"},
			{"action": "rename", "value": "2-modified"},
			{"action": "addChild", "value": "4,5"},
		},
		3: {
			{"action": "addChild", "value": "11,12"},
			{"action": "rename", "value": "3-modified"},
			{"action": "move", "value": "11:/0"},
		},
	}

	for i := 0; i < len(data); i++ {
		process := data[i]
		name := strconv.Itoa(i)
		renameList = append(renameList, name)
		for _, action := range process {
			value := action["value"]
			if action["action"] == "addChild" {
				childNames := strings.Split(value, ",")
				for _, childName := range childNames {
					address := aFilerUrl + "/" + name + "/" + childName
					createFile1KB(client, address, buffers)
				}
			} else if action["action"] == "deleteChild" {
				childNames := strings.Split(value, ",")
				for _, childName := range childNames {
					address := aFilerUrl + "/" + name + "/" + childName
					deleteFilesOrFolders(client, address)
				}
			} else if action["action"] == "delete" {
				address := aFilerUrl + "/" + name + "?recursive=true&ignoreRecursiveError=true"
				deleteFilesOrFolders(client, address)
			} else if action["action"] == "rename" {
				moveAddress := aFilerUrl + "/" + value + "?mv.from=" + rootPath + "/" + name
				mvFilesOrFolders(client, moveAddress)
				name = value
				renameList = append(renameList, name)
			} else if action["action"] == "move" {
				operatorArray := strings.Split(value, ":")
				childNames := strings.Split(operatorArray[0], ",")
				targetFolder := operatorArray[1]
				for _, childName := range childNames {
					moveAddress := aFilerUrl + targetFolder + "/" + childName + "?mv.from=" + rootPath + "/" + name + "/" + childName
					mvFilesOrFolders(client, moveAddress)
				}
			}
		}
	}

	// delete all affected files
	if deleteFolder {
		fmt.Println("DELETE Folder")
		for i := 0; i < len(renameList); i++ {
			address := aFilerUrl + "/" + renameList[i] + "?recursive=true&ignoreRecursiveError=true"
			deleteFilesOrFolders(client, address)
		}
	}

	printCostTime(startTime)
}

// move 0~9 to 10
func TestParallelSyncMove(t *testing.T) {
	t.SkipNow()
	fileFolderNumber := 10
	fileNumber := 1
	deleteFolder := true
	rootPath := "/test"
	aFilerUrl := "http://localhost:8888" + rootPath
	buffers, _ := getFile1KBBytes()
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
	startTime := time.Now()
	for i := 0; i < fileFolderNumber+1; i++ {
		if i == fileFolderNumber {
			address := aFilerUrl + "/" + strconv.Itoa(i) + "/"
			createFolder(client, address)
			break
		}
		// create filerNumber files
		for j := 0; j < fileNumber; j++ {
			address := aFilerUrl + "/" + strconv.Itoa(i) + "/" + strconv.Itoa(j)
			createFile1KB(client, address, buffers)
		}
	}
	for i := 0; i < fileFolderNumber; i++ {
		// move filerNumber files to "10"
		for j := 0; j < fileNumber; j++ {
			moveAddress := aFilerUrl + "/" + strconv.Itoa(fileFolderNumber) + "/" + strconv.Itoa(i) + "?mv.from=" + rootPath + "/" + strconv.Itoa(i) + "/0"
			mvFilesOrFolders(client, moveAddress)
		}
	}
	// delete all files
	if deleteFolder {
		for i := 0; i < fileFolderNumber; i++ {
			address := aFilerUrl + "/" + strconv.Itoa(i) + "/"
			deleteFilesOrFolders(client, address)
		}
	}
	printCostTime(startTime)

}

func TestParallelSyncEvents(t *testing.T) {
	var parallelNum = 10
	var parallelBatch = 100
	var persistEventFns = make([]func(resp *filer_pb.SubscribeMetadataResponse) error, 0, parallelNum)
	var folderCount = 10
	var fileCount = 10
	var deleteFilerCount = folderCount*fileCount + folderCount
	var totalCount = folderCount*folderCount + folderCount + deleteFilerCount
	var sendCount = 0
	var goroutinePids []string
	var sortEvents []*filer_pb.SubscribeMetadataResponse
	var startTime = time.Now()
	recordMap := make(map[string][]string)

	lock := sync.RWMutex{}

	for i := 0; i < parallelNum; i++ {
		persistEventFns = append(persistEventFns, func(resp *filer_pb.SubscribeMetadataResponse) error {
			lock.Lock()
			pid := string(bytes.Fields(debug.Stack())[1])
			exist := false
			for _, e := range goroutinePids {
				if e == pid {
					exist = true
				}
			}
			if !exist {
				goroutinePids = append(goroutinePids, pid)
			}
			recordMap[pid] = append(recordMap[pid], resp.String())
			sortEvents = append(sortEvents, resp)
			lock.Unlock()
			return nil
		})
	}

	stopEventsConsumerChan := make(chan struct{})

	setOffsetFn := func(counter int64, lastTsNs int64) error {
		sendCount = sendCount + int(counter)
		return nil
	}

	eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 100)

	cache := &ParallelSyncMetadataCache{persistEventFns: persistEventFns,
		events: []*filer_pb.SubscribeMetadataResponse{}, eventsChan: eventsChan, cancelChan: stopEventsConsumerChan,
		parallelNum: parallelNum, parallelBatchSize: parallelBatch, parallelWaitTime: 2 * time.Second,
		sourceFiler: "sourceFiler", targetFiler: "targetFiler"}

	go startEventsConsumer(cache, setOffsetFn)

	// put event into channel
	for i := 0; i < folderCount; i++ {
		curTimeNs := time.Now().UnixNano()
		newEntry := filer_pb.Entry{Name: "/" + strconv.Itoa(i)}
		eventNotification := filer_pb.EventNotification{NewEntry: &newEntry, OldEntry: nil}
		event := filer_pb.SubscribeMetadataResponse{TsNs: curTimeNs, EventNotification: &eventNotification}
		eventsChan <- &event
		for j := 0; j < fileCount; j++ {
			newEntry := filer_pb.Entry{Name: "/" + strconv.Itoa(i) + "/" + strconv.Itoa(j)}
			eventNotification := filer_pb.EventNotification{NewEntry: &newEntry, OldEntry: nil}
			event := filer_pb.SubscribeMetadataResponse{TsNs: curTimeNs, EventNotification: &eventNotification}
			eventsChan <- &event
		}
	}

	for i := 0; i < folderCount; i++ {
		curTimeNs := time.Now().UnixNano()
		for j := 0; j < fileCount; j++ {
			// delete files
			oldEntry := filer_pb.Entry{Name: "/" + strconv.Itoa(i) + "/" + strconv.Itoa(j)}
			eventNotification := filer_pb.EventNotification{NewEntry: nil, OldEntry: &oldEntry}
			event := filer_pb.SubscribeMetadataResponse{TsNs: curTimeNs, EventNotification: &eventNotification}
			eventsChan <- &event
		}
		oldEntry := filer_pb.Entry{Name: "/" + strconv.Itoa(i)}
		eventNotification := filer_pb.EventNotification{NewEntry: nil, OldEntry: &oldEntry}
		event := filer_pb.SubscribeMetadataResponse{TsNs: curTimeNs, EventNotification: &eventNotification}
		eventsChan <- &event
	}

	// check event size
	// Exit automatically for more than 50 times
	maxWaitTimes := 50
	for {
		if maxWaitTimes == 0 {
			stopEventsConsumerChan <- struct{}{}
			fmt.Printf("automatically stop event synchronization consumer. from %s to %s\n", "sourceFiler", "targetFiler")
			break
		}

		if totalCount == sendCount {
			stopEventsConsumerChan <- struct{}{}
			fmt.Printf("stop event synchronization consumer. from %s to %s\n", "sourceFiler", "targetFiler")
			break
		}
		maxWaitTimes = maxWaitTimes - 1
		time.Sleep(200 * time.Millisecond)
	}

	// add action
	for i := 0; i < folderCount; i++ {
		assert.True(t, ifBeforeAllEventsByTsNs(i, false, sortEvents))
	}

	// delete action
	reverseSlice(sortEvents)
	for i := 0; i < folderCount; i++ {
		assert.True(t, ifBeforeAllEventsByTsNs(i, true, sortEvents))
	}

	printCostTime(startTime)
}

func reverseSlice(s interface{}) {
	size := reflect.ValueOf(s).Len()
	swap := reflect.Swapper(s)
	for i, j := 0, size-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}
}

func ifBeforeAllEventsByTsNs(idx int, ifDelete bool, events []*filer_pb.SubscribeMetadataResponse) bool {
	// find the same timestamp to judge whether it is the first one.
	for _, item := range events {
		if !ifDelete {
			if item.EventNotification.NewEntry != nil {
				if strings.HasPrefix(item.EventNotification.NewEntry.Name, "/"+strconv.Itoa(idx)) {
					// add action
					if item.EventNotification.NewEntry.Name == "/"+strconv.Itoa(idx) {
						return true
					} else {
						return false
					}
				}
			}
		} else {
			// delete action
			if item.EventNotification.OldEntry != nil {
				if strings.HasPrefix(item.EventNotification.OldEntry.Name, "/"+strconv.Itoa(idx)) {
					if item.EventNotification.OldEntry.Name == "/"+strconv.Itoa(idx) {
						return true
					} else {
						return false
					}
				}
			}
		}
	}
	return false
}

func TestParallelSyncSplitNodes(t *testing.T) {
	// a total of 13 numbers
	// The asterisk indicates that this node has been operated.
	rootTree := ParallelSyncNode{fullPathName: "/", fullPath: []string{}, curPathName: ""}
	//              /
	//    1         5        10
	//   2*       6 7* 8      11*
	//  3* 4     9*            12
	basePoint := [][]string{
		0: {"1", "2", "3"},
		1: {"1", "2", "4"},
		2: {"5", "7"},
		3: {"5", "8"},
		4: {"5", "6", "9"},
		5: {"10", "11", "12"},
	}
	// hit number
	hitPoint := [][]string{
		0: {"1", "2"},
		1: {"1", "2", "3"},
		2: {"5", "7"},
		3: {"5", "6", "9"},
		4: {"10", "11"},
		5: {"10", "11", "12"},
	}
	for _, v := range basePoint {
		rootTree.addNode(-1, v)
	}

	for _, v := range hitPoint {
		a, _ := strconv.Atoi(v[len(v)-1])
		rootTree.addNode(a, v)
	}

	var list []*filer_pb.SubscribeMetadataResponse
	for i := 0; i < 13; i++ {
		item := filer_pb.SubscribeMetadataResponse{
			Directory: strconv.Itoa(i),
		}
		list = append(list, &item)
	}

	var workerGroupResultArray [][]int
	getEventIndexesByNode(rootTree, &workerGroupResultArray)

	assert.EqualValues(t, workerGroupResultArray[0], []int{2, 3})
	assert.EqualValues(t, workerGroupResultArray[1], []int{7})
	assert.EqualValues(t, workerGroupResultArray[2], []int{9})
	assert.EqualValues(t, workerGroupResultArray[3], []int{11, 12})
}

func TestGetMinLenIdxFromWorkerGroup(t *testing.T) {
	var workerGroup = make([][]*filer_pb.SubscribeMetadataResponse, 5)

	data1 := []int{
		5, 4, 3, 2, 1,
	}

	for i := 0; i < data1[0]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[0] = append(workerGroup[0], tmp)
	}

	for i := 0; i < data1[1]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[1] = append(workerGroup[1], tmp)
	}

	for i := 0; i < data1[2]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[2] = append(workerGroup[2], tmp)
	}

	for i := 0; i < data1[3]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[3] = append(workerGroup[3], tmp)
	}

	for i := 0; i < data1[4]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[4] = append(workerGroup[4], tmp)
	}

	result := getWorkerGroupMinLengthIndex(workerGroup)
	assert.Equal(t, 4, result)

	data2 := []int{
		3, 4, 1, 2, 5,
	}

	for i := 0; i < data2[0]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[0] = append(workerGroup[0], tmp)
	}

	for i := 0; i < data2[1]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[1] = append(workerGroup[1], tmp)
	}

	for i := 0; i < data2[2]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[2] = append(workerGroup[2], tmp)
	}

	for i := 0; i < data2[3]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[3] = append(workerGroup[3], tmp)
	}

	for i := 0; i < data2[4]; i++ {
		var tmp *filer_pb.SubscribeMetadataResponse
		workerGroup[4] = append(workerGroup[4], tmp)
	}

	result = getWorkerGroupMinLengthIndex(workerGroup)
	assert.Equal(t, 2, result)
}

func createFolder(client *http.Client, address string) {
	request, err := http.NewRequest("POST", address, nil)
	resp, err := client.Do(request)
	if err != err {
		fmt.Printf("post %s error.", address)
	}
	resp.Body.Close()
}

func getFile1KBBytes() ([]byte, error) {
	filename := "./filer_sync_test.go"
	// create a 1kb file
	buffer := make([]byte, 1024)
	file, err := os.Open(filename)
	if err != nil {
		return buffer, err
	}
	defer file.Close()
	for {
		_, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}
	return buffer, err
}

func createFile1KB(client *http.Client, address string, buffer []byte) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "filer_sync_test.go")
	part.Write(buffer)
	err = writer.Close()
	request, err := http.NewRequest("POST", address, body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := client.Do(request)
	resp.Body.Close()
	if err != err {
		fmt.Printf("post %s error.", address)
	}
}

func deleteFilesOrFolders(client *http.Client, address string) {
	request, err := http.NewRequest("DELETE", address, nil)
	resp, err := client.Do(request)
	if err != nil {
		fmt.Printf("delete %s error.", address)
	}
	resp.Body.Close()
}

func mvFilesOrFolders(client *http.Client, address string) {
	request, err := http.NewRequest("POST", address, nil)
	resp, err := client.Do(request)
	if err != nil {
		fmt.Printf("delete %s error.", address)
	}
	resp.Body.Close()
}

func printCostTime(startTime time.Time) {
	fmt.Printf("cost: %0.2fs\n", time.Since(startTime).Seconds())
}
