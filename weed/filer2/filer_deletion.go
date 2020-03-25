package filer2

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func LookupByMasterClientFn(masterClient *wdclient.MasterClient) func(vids []string) (map[string]operation.LookupResult, error) {
	return func(vids []string) (map[string]operation.LookupResult, error) {
		m := make(map[string]operation.LookupResult)
		for _, vid := range vids {
			locs, _ := masterClient.GetVidLocations(vid)
			var locations []operation.Location
			for _, loc := range locs {
				locations = append(locations, operation.Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
				})
			}
			m[vid] = operation.LookupResult{
				VolumeId:  vid,
				Locations: locations,
			}
		}
		return m, nil
	}
}

func (f *Filer) loopProcessingDeletion() {

	lookupFunc := LookupByMasterClientFn(f.MasterClient)

	var deletionCount int
	for {
		deletionCount = 0
		f.fileIdDeletionQueue.Consume(func(fileIds []string) {
			deletionCount = len(fileIds)
			deleteResults, err := operation.DeleteFilesWithLookupVolumeId(f.GrpcDialOption, fileIds, lookupFunc)
			if err != nil {
				glog.V(0).Infof("deleting fileIds len=%d error: %v", deletionCount, err)
			} else {
				glog.V(1).Infof("deleting fileIds len=%d", deletionCount)
			}
			if len(deleteResults) != deletionCount {
				glog.V(0).Infof("delete %d fileIds actual %d", deletionCount, len(deleteResults))
			}
		})

		if deletionCount == 0 {
			time.Sleep(1123 * time.Millisecond)
		}
	}
}

func (f *Filer) DeleteChunks(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
	}
}

// DeleteFileByFileId direct delete by file id.
// Only used when the fileId is not being managed by snapshots.
func (f *Filer) DeleteFileByFileId(fileId string) {
	f.fileIdDeletionQueue.EnQueue(fileId)
}

func (f *Filer) deleteChunksIfNotNew(oldEntry, newEntry *Entry) {

	if oldEntry == nil {
		return
	}
	if newEntry == nil {
		f.DeleteChunks(oldEntry.Chunks)
	}

	var toDelete []*filer_pb.FileChunk
	newChunkIds := make(map[string]bool)
	for _, newChunk := range newEntry.Chunks {
		newChunkIds[newChunk.GetFileIdString()] = true
	}

	for _, oldChunk := range oldEntry.Chunks {
		if _, found := newChunkIds[oldChunk.GetFileIdString()]; !found {
			toDelete = append(toDelete, oldChunk)
		}
	}
	f.DeleteChunks(toDelete)
}
