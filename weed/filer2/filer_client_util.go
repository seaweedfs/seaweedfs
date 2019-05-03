package filer2

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func VolumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}

type FilerClient interface {
	WithFilerClient(ctx context.Context, fn func(filer_pb.SeaweedFilerClient) error) error
}

func ReadIntoBuffer(ctx context.Context, filerClient FilerClient, fullFilePath string, buff []byte, chunkViews []*ChunkView, baseOffset int64) (totalRead int64, err error) {
	var vids []string
	for _, chunkView := range chunkViews {
		vids = append(vids, VolumeId(chunkView.FileId))
	}

	vid2Locations := make(map[string]*filer_pb.Locations)

	err = filerClient.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		glog.V(4).Infof("read fh lookup volume id locations: %v", vids)
		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
			VolumeIds: vids,
		})
		if err != nil {
			return err
		}

		vid2Locations = resp.LocationsMap

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to lookup volume ids %v: %v", vids, err)
	}

	var wg sync.WaitGroup
	for _, chunkView := range chunkViews {
		wg.Add(1)
		go func(chunkView *ChunkView) {
			defer wg.Done()

			glog.V(4).Infof("read fh reading chunk: %+v", chunkView)

			locations := vid2Locations[VolumeId(chunkView.FileId)]
			if locations == nil || len(locations.Locations) == 0 {
				glog.V(0).Infof("failed to locate %s", chunkView.FileId)
				err = fmt.Errorf("failed to locate %s", chunkView.FileId)
				return
			}

			var n int64
			n, err = util.ReadUrl(
				fmt.Sprintf("http://%s/%s", locations.Locations[0].Url, chunkView.FileId),
				chunkView.Offset,
				int(chunkView.Size),
				buff[chunkView.LogicOffset-baseOffset:chunkView.LogicOffset-baseOffset+int64(chunkView.Size)],
				!chunkView.IsFullChunk)

			if err != nil {

				glog.V(0).Infof("%v read http://%s/%v %v bytes: %v", fullFilePath, locations.Locations[0].Url, chunkView.FileId, n, err)

				err = fmt.Errorf("failed to read http://%s/%s: %v",
					locations.Locations[0].Url, chunkView.FileId, err)
				return
			}

			glog.V(4).Infof("read fh read %d bytes: %+v", n, chunkView)
			totalRead += n

		}(chunkView)
	}
	wg.Wait()
	return
}

func GetEntry(ctx context.Context, filerClient FilerClient, fullFilePath string) (entry *filer_pb.Entry, err error) {

	dir, name := FullPath(fullFilePath).DirAndName()

	err = filerClient.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		glog.V(1).Infof("read %s request: %v", fullFilePath, request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			if err == ErrNotFound {
				return nil
			}
			glog.V(0).Infof("read %s attr %v: %v", fullFilePath, request, err)
			return err
		}

		if resp.Entry != nil {
			entry = resp.Entry
		}

		return nil
	})

	return
}

func ReadDirAllEntries(ctx context.Context, filerClient FilerClient, fullDirPath string, fn func(entry *filer_pb.Entry)) (err error) {

	err = filerClient.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		paginationLimit := 1024

		lastEntryName := ""

		for {

			request := &filer_pb.ListEntriesRequest{
				Directory:         fullDirPath,
				StartFromFileName: lastEntryName,
				Limit:             uint32(paginationLimit),
			}

			glog.V(4).Infof("read directory: %v", request)
			resp, err := client.ListEntries(ctx, request)
			if err != nil {
				return fmt.Errorf("list %s: %v", fullDirPath, err)
			}

			for _, entry := range resp.Entries {
				fn(entry)
				lastEntryName = entry.Name
			}

			if len(resp.Entries) < paginationLimit {
				break
			}

		}

		return nil

	})

	return
}
