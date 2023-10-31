package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{}
	Commands = append(Commands, &commandFsMergeVolumes{})
}

type commandFsMergeVolumes struct {
	volumes map[needle.VolumeId]*master_pb.VolumeInformationMessage
}

func (c *commandFsMergeVolumes) Name() string {
	return "fs.mergeVolumes"
}

func (c *commandFsMergeVolumes) Help() string {
	return `re-locate chunks into target volumes and try to clear lighter volumes.
	
	This would help clear half-full volumes and let vacuum system to delete them later.

	fs.mergeVolumes -toVolumeId=y [-fromVolumeId=x] [-apply] /dir/
`
}

func (c *commandFsMergeVolumes) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	dir, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}
	dir = strings.TrimRight(dir, "/")
	fsMergeVolumesCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	fromVolumeArg := fsMergeVolumesCommand.Uint("fromVolumeId", 0, "move chunks with this volume id")
	toVolumeArg := fsMergeVolumesCommand.Uint("toVolumeId", 0, "change chunks to this volume id")
	apply := fsMergeVolumesCommand.Bool("apply", false, "applying the metadata changes")
	if err = fsMergeVolumesCommand.Parse(args); err != nil {
		return err
	}
	fromVolumeId := needle.VolumeId(*fromVolumeArg)
	toVolumeId := needle.VolumeId(*toVolumeArg)

	if toVolumeId == 0 {
		return fmt.Errorf("volume id can not be zero")
	}

	c.reloadVolumesInfo(commandEnv.MasterClient)

	toVolumeInfo, err := c.getVolumeInfoById(toVolumeId)
	if err != nil {
		return err
	}
	if toVolumeInfo.ReadOnly {
		return fmt.Errorf("volume is readonly: %d", toVolumeId)
	}

	if fromVolumeId != 0 {
		if fromVolumeId == toVolumeId {
			return fmt.Errorf("no volume id changes, %d == %d", fromVolumeId, toVolumeId)
		}
		compatible, err := c.volumesAreCompatible(fromVolumeId, toVolumeId)
		if err != nil {
			return fmt.Errorf("cannot determine volumes are compatible: %d and %d", fromVolumeId, toVolumeId)
		}
		if !compatible {
			return fmt.Errorf("volume %d is not compatible with volume %d", fromVolumeId, toVolumeId)
		}
	}
	defer client.CloseIdleConnections()

	compatibility := make(map[string]bool)

	return commandEnv.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(commandEnv, util.FullPath(dir), func(parentPath util.FullPath, entry *filer_pb.Entry) {
			if !entry.IsDirectory {
				for _, chunk := range entry.Chunks {
					if chunk.IsChunkManifest {
						fmt.Printf("Change volume id for large file is not implemented yet: %s/%s\n", parentPath, entry.Name)
						continue
					}
					chunkVolumeId := needle.VolumeId(chunk.Fid.VolumeId)
					if chunkVolumeId == toVolumeId || (fromVolumeId != 0 && fromVolumeId != chunkVolumeId) {
						continue
					}
					cacheKey := fmt.Sprintf("%d-%d", chunkVolumeId, toVolumeId)
					compatible, cached := compatibility[cacheKey]
					if !cached {
						compatible, err = c.volumesAreCompatible(chunkVolumeId, toVolumeId)
						if err != nil {
							_ = fmt.Errorf("cannot determine volumes are compatible: %d and %d", chunkVolumeId, toVolumeId)
							return
						}
						compatibility[cacheKey] = compatible
					}
					if !compatible {
						if fromVolumeId != 0 {
							_ = fmt.Errorf("volumes are incompatible: %d and %d", fromVolumeId, toVolumeId)
							return
						}
						continue
					}
					path := parentPath.Child(entry.Name)

					fmt.Printf("move %s(%s)\n", path, chunk.GetFileIdString())
					if !*apply {
						continue
					}
					if err = moveChunk(chunk, toVolumeId, commandEnv.MasterClient); err != nil {
						fmt.Printf("failed to move %s/%s: %v\n", path, chunk.GetFileIdString(), err)
						continue
					}

					if err = filer_pb.UpdateEntry(filerClient, &filer_pb.UpdateEntryRequest{
						Directory: string(parentPath),
						Entry:     entry,
					}); err != nil {
						fmt.Printf("failed to update %s: %v\n", path, err)
					}
				}
			}
		})
	})
}

func (c *commandFsMergeVolumes) getVolumeInfoById(vid needle.VolumeId) (*master_pb.VolumeInformationMessage, error) {
	info := c.volumes[vid]
	var err error
	if info == nil {
		err = errors.New("cannot find volume")
	}
	return info, err
}

func (c *commandFsMergeVolumes) volumesAreCompatible(src needle.VolumeId, dest needle.VolumeId) (bool, error) {
	srcInfo, err := c.getVolumeInfoById(src)
	if err != nil {
		return false, err
	}
	destInfo, err := c.getVolumeInfoById(dest)
	if err != nil {
		return false, err
	}
	return (srcInfo.Collection == destInfo.Collection &&
		srcInfo.Ttl == destInfo.Ttl &&
		srcInfo.ReplicaPlacement == destInfo.ReplicaPlacement), nil
}

func (c *commandFsMergeVolumes) reloadVolumesInfo(masterClient *wdclient.MasterClient) error {
	c.volumes = make(map[needle.VolumeId]*master_pb.VolumeInformationMessage)

	return masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		volumes, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}
		for _, dc := range volumes.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for _, disk := range node.DiskInfos {
						for _, volume := range disk.VolumeInfos {
							vid := needle.VolumeId(volume.Id)
							if found := c.volumes[vid]; found == nil {
								c.volumes[vid] = volume
							}
						}
					}
				}
			}
		}
		return nil
	})
}

func moveChunk(chunk *filer_pb.FileChunk, toVolumeId needle.VolumeId, masterClient *wdclient.MasterClient) error {
	fromFid := needle.NewFileId(needle.VolumeId(chunk.Fid.VolumeId), chunk.Fid.FileKey, chunk.Fid.Cookie)
	toFid := needle.NewFileId(toVolumeId, chunk.Fid.FileKey, chunk.Fid.Cookie)

	downloadURLs, err := masterClient.LookupVolumeServerUrl(fromFid.VolumeId.String())
	if err != nil {
		return err
	}

	downloadURL := fmt.Sprintf("http://%s/%s", downloadURLs[0], fromFid.String())

	uploadURLs, err := masterClient.LookupVolumeServerUrl(toVolumeId.String())
	if err != nil {
		return err
	}
	uploadURL := fmt.Sprintf("http://%s/%s", uploadURLs[0], toFid.String())

	resp, reader, err := readUrl(downloadURL)
	if err != nil {
		return err
	}
	defer util.CloseResponse(resp)
	defer reader.Close()

	var filename string

	contentDisposition := resp.Header.Get("Content-Disposition")
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition, "filename=")
		if idx != -1 {
			filename = contentDisposition[idx+len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}

	contentType := resp.Header.Get("Content-Type")
	isCompressed := resp.Header.Get("Content-Encoding") == "gzip"
	md5 := resp.Header.Get("Content-MD5")

	_, err, _ = operation.Upload(reader, &operation.UploadOption{
		UploadUrl:         uploadURL,
		Filename:          filename,
		IsInputCompressed: isCompressed,
		Cipher:            false,
		MimeType:          contentType,
		PairMap:           nil,
		Md5:               md5,
	})
	if err != nil {
		return err
	}
	chunk.Fid.VolumeId = uint32(toVolumeId)
	chunk.FileId = ""

	return nil
}

func readUrl(fileUrl string) (*http.Response, io.ReadCloser, error) {

	req, err := http.NewRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Accept-Encoding", "gzip")

	r, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if r.StatusCode >= 400 {
		util.CloseResponse(r)
		return nil, nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	return r, r.Body, nil
}
