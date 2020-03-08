package filer2

import (
	"context"
	"fmt"
	"io"
	"math"
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
	WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error
	AdjustedUrl(hostAndPort string) string
}

func ReadIntoBuffer(filerClient FilerClient, fullFilePath FullPath, buff []byte, chunkViews []*ChunkView, baseOffset int64) (totalRead int64, err error) {
	var vids []string
	for _, chunkView := range chunkViews {
		vids = append(vids, VolumeId(chunkView.FileId))
	}

	vid2Locations := make(map[string]*filer_pb.Locations)

	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		glog.V(4).Infof("read fh lookup volume id locations: %v", vids)
		resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
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

			volumeServerAddress := filerClient.AdjustedUrl(locations.Locations[0].Url)
			var n int64
			n, err = util.ReadUrl(fmt.Sprintf("http://%s/%s", volumeServerAddress, chunkView.FileId), chunkView.CipherKey, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), buff[chunkView.LogicOffset-baseOffset:chunkView.LogicOffset-baseOffset+int64(chunkView.Size)])

			if err != nil {

				glog.V(0).Infof("%v read http://%s/%v %v bytes: %v", fullFilePath, volumeServerAddress, chunkView.FileId, n, err)

				err = fmt.Errorf("failed to read http://%s/%s: %v",
					volumeServerAddress, chunkView.FileId, err)
				return
			}

			glog.V(4).Infof("read fh read %d bytes: %+v", n, chunkView)
			totalRead += n

		}(chunkView)
	}
	wg.Wait()
	return
}

func GetEntry(filerClient FilerClient, fullFilePath FullPath) (entry *filer_pb.Entry, err error) {

	dir, name := fullFilePath.DirAndName()

	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		// glog.V(3).Infof("read %s request: %v", fullFilePath, request)
		resp, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			glog.V(3).Infof("read %s %v: %v", fullFilePath, resp, err)
			return err
		}

		if resp.Entry == nil {
			// glog.V(3).Infof("read %s entry: %v", fullFilePath, entry)
			return nil
		}

		entry = resp.Entry
		return nil
	})

	return
}

func ReadDirAllEntries(filerClient FilerClient, fullDirPath FullPath, prefix string, fn func(entry *filer_pb.Entry, isLast bool)) (err error) {

	err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		lastEntryName := ""

		request := &filer_pb.ListEntriesRequest{
			Directory:         string(fullDirPath),
			Prefix:            prefix,
			StartFromFileName: lastEntryName,
			Limit:             math.MaxUint32,
		}

		glog.V(3).Infof("read directory: %v", request)
		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list %s: %v", fullDirPath, err)
		}

		var prevEntry *filer_pb.Entry
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					if prevEntry != nil {
						fn(prevEntry, true)
					}
					break
				} else {
					return recvErr
				}
			}
			if prevEntry != nil {
				fn(prevEntry, false)
			}
			prevEntry = resp.Entry
		}

		return nil

	})

	return
}
