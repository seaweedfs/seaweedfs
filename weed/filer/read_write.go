package filer

import (
	"bytes"
	"context"
	"crypto/md5"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func ReadEntry(masterClient *wdclient.MasterClient, filerClient filer_pb.SeaweedFilerClient, dir, name string, byteBuffer *bytes.Buffer) error {

	request := &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	}
	respLookupEntry, err := filer_pb.LookupEntry(context.Background(), filerClient, request)
	if err != nil {
		return err
	}
	if len(respLookupEntry.Entry.Content) > 0 {
		_, err = byteBuffer.Write(respLookupEntry.Entry.Content)
		return err
	}

	return StreamContent(masterClient, byteBuffer, respLookupEntry.Entry.GetChunks(), 0, int64(FileSize(respLookupEntry.Entry)))

}

func ReadInsideFiler(ctx context.Context, filerClient filer_pb.SeaweedFilerClient, dir, name string) (content []byte, err error) {
	request := &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	}
	respLookupEntry, err := filer_pb.LookupEntry(ctx, filerClient, request)
	if err != nil {
		return
	}
	content = respLookupEntry.Entry.Content
	return
}

// SaveInsideFiler writes content into the entry's inline Content. The content
// MD5 is stamped on every write so the entry's ETag always describes what is
// actually stored: conditional writers key IF_ETAG_MATCH off it, and a hash
// left behind by an earlier write would make that condition pass against
// content that has since changed.
func SaveInsideFiler(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string, content []byte) error {

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})

	contentMd5 := md5.Sum(content)

	if err == filer_pb.ErrNotFound {
		err = filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0644),
					FileSize: uint64(len(content)),
					Md5:      contentMd5[:],
				},
				Content: content,
			},
			SkipCheckParentDirectory: false,
		})
	} else if err == nil {
		entry := resp.Entry
		entry.Content = content
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileSize = uint64(len(content))
		entry.Attributes.Md5 = contentMd5[:]
		err = filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
	}

	return err
}
