package filer2

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
				operation.DeleteFilesWithLookupVolumeId(f.GrpcDialOption, fileIds, lookupFunc)
				fileIds = fileIds[:0]
			}
		case <-ticker.C:
			if len(fileIds) > 0 {
				glog.V(1).Infof("timed deletion fileIds len=%d", len(fileIds))
				operation.DeleteFilesWithLookupVolumeId(f.GrpcDialOption, fileIds, lookupFunc)
				fileIds = fileIds[:0]
			}
		}
	}
}

func (f *Filer) DeleteChunks(fullpath FullPath, chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		glog.V(3).Infof("deleting %s chunk %s", fullpath, chunk.String())
		f.fileIdDeletionChan <- chunk.FileId
	}
}

// DeleteFileByFileId direct delete by file id.
// Only used when the fileId is not being managed by snapshots.
func (f *Filer) DeleteFileByFileId(fileId string) {
	f.fileIdDeletionChan <- fileId
}

func (f *Filer) deleteChunksIfNotNew(oldEntry, newEntry *Entry) {

	if oldEntry == nil {
		return
	}
	if newEntry == nil {
		f.DeleteChunks(oldEntry.FullPath, oldEntry.Chunks)
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
	f.DeleteChunks(oldEntry.FullPath, toDelete)
}
