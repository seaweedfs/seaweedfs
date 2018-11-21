package filer2

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (f *Filer) loopProcessingDeletion() {

	ticker := time.NewTicker(5 * time.Second)

	lookupFunc := func(vids []string) (map[string]operation.LookupResult, error) {
		m := make(map[string]operation.LookupResult)
		for _, vid := range vids {
			locs := f.MasterClient.GetVidLocations(vid)
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

	var fileIds []string
	for {
		select {
		case fid := <-f.fileIdDeletionChan:
			fileIds = append(fileIds, fid)
			if len(fileIds) >= 4096 {
				glog.V(1).Infof("deleting fileIds len=%d", len(fileIds))
				operation.DeleteFilesWithLookupVolumeId(fileIds, lookupFunc)
				fileIds = fileIds[:0]
			}
		case <-ticker.C:
			if len(fileIds) > 0 {
				glog.V(1).Infof("timed deletion fileIds len=%d", len(fileIds))
				operation.DeleteFilesWithLookupVolumeId(fileIds, lookupFunc)
				fileIds = fileIds[:0]
			}
		}
	}
}

func (f *Filer) DeleteChunks(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		f.fileIdDeletionChan <- chunk.FileId
	}
}

func (f *Filer) DeleteFileByFileId(fileId string) {
	f.fileIdDeletionChan <- fileId
}

func (f *Filer) deleteChunksIfNotNew(oldEntry, newEntry *Entry) {

	if oldEntry == nil {
		return
	}
	if newEntry == nil {
		f.DeleteChunks(oldEntry.Chunks)
	}

	var toDelete []*filer_pb.FileChunk

	for _, oldChunk := range oldEntry.Chunks {
		found := false
		for _, newChunk := range newEntry.Chunks {
			if oldChunk.FileId == newChunk.FileId {
				found = true
				break
			}
		}
		if !found {
			toDelete = append(toDelete, oldChunk)
		}
	}
	f.DeleteChunks(toDelete)
}
