package filer_pb

import (
	"github.com/joeslay/seaweedfs/weed/storage/needle"
)

func toFileIdObject(fileIdStr string) (*FileId, error) {
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

func (fid *FileId) toFileIdString() string {
	return needle.NewFileId(needle.VolumeId(fid.VolumeId), fid.FileKey, fid.Cookie).String()
}

func (c *FileChunk) GetFileIdString() string {
	if c.FileId != "" {
		return c.FileId
	}
	if c.Fid != nil {
		c.FileId = c.Fid.toFileIdString()
		return c.FileId
	}
	return ""
}

func BeforeEntrySerialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.FileId != "" {
			if fid, err := toFileIdObject(chunk.FileId); err == nil {
				chunk.Fid = fid
				chunk.FileId = ""
			}
		}

		if chunk.SourceFileId != "" {
			if fid, err := toFileIdObject(chunk.SourceFileId); err == nil {
				chunk.SourceFid = fid
				chunk.SourceFileId = ""
			}
		}

	}
}

func AfterEntryDeserialization(chunks []*FileChunk) {

	for _, chunk := range chunks {

		if chunk.Fid != nil && chunk.FileId == "" {
			chunk.FileId = chunk.Fid.toFileIdString()
		}

		if chunk.SourceFid != nil && chunk.SourceFileId == "" {
			chunk.SourceFileId = chunk.SourceFid.toFileIdString()
		}

	}
}
