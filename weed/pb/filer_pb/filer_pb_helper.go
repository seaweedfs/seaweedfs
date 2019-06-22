package filer_pb

import (
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func toFileId(fileIdStr string) (*FileId, error) {
	t, err := needle.ParseFileIdFromString(fileIdStr)
	if err != nil {
		return nil, err
	}
	return &FileId{
		VolumeId: uint32(t.VolumeId),
		Cookie:   uint32(t.Cookie),
		FileKey:  uint64(t.Key),
	}, nil

}

func (fid *FileId) toFileId() string {
	return needle.NewFileId(needle.VolumeId(fid.VolumeId), fid.FileKey, fid.Cookie).String()
}

func ChunkEquals(this, that *FileChunk) bool {
	if this.Fid == nil{
		this.Fid, _ = toFileId(this.FileId)
	}
	if that.Fid == nil{
		that.Fid, _ = toFileId(that.FileId)
	}
	return this.Fid.FileKey == that.Fid.FileKey && this.Fid.VolumeId == that.Fid.VolumeId && this.Fid.Cookie == that.Fid.Cookie
}

func BeforeEntrySerialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.FileId != "" {
			if fid, err := toFileId(chunk.FileId); err == nil {
				chunk.Fid = fid
				chunk.FileId = ""
			}
		}

		if chunk.SourceFileId != "" {
			if fid, err := toFileId(chunk.SourceFileId); err == nil {
				chunk.SourceFid = fid
				chunk.SourceFileId = ""
			}
		}

	}
}

func AfterEntryDeserialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.Fid != nil && chunk.FileId == "" {
			chunk.FileId = chunk.Fid.toFileId()
		}

		if chunk.SourceFid != nil && chunk.SourceFileId == "" {
			chunk.SourceFileId = chunk.SourceFid.toFileId()
		}

	}
}
