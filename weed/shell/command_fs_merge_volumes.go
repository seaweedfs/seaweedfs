package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/exp/maps"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func init() {
	Commands = append(Commands, &commandFsMergeVolumes{})
}

type commandFsMergeVolumes struct {
	volumes         map[needle.VolumeId]*master_pb.VolumeInformationMessage
	volumeSizeLimit uint64
}

func (c *commandFsMergeVolumes) Name() string {
	return "fs.mergeVolumes"
}

func (c *commandFsMergeVolumes) Help() string {
	return `re-locate chunks into target volumes and try to clear lighter volumes.
	
	This would help clear half-full volumes and let vacuum system to delete them later.

	fs.mergeVolumes [-toVolumeId=y] [-fromVolumeId=x] [-collection="*"] [-dir=/] [-apply]
`
}

func (c *commandFsMergeVolumes) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMergeVolumes) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMergeVolumesCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	dirArg := fsMergeVolumesCommand.String("dir", "/", "base directory to find and update files")
	fromVolumeArg := fsMergeVolumesCommand.Uint("fromVolumeId", 0, "move chunks with this volume id")
	toVolumeArg := fsMergeVolumesCommand.Uint("toVolumeId", 0, "change chunks to this volume id")
	collectionArg := fsMergeVolumesCommand.String("collection", "*", "Name of collection to merge")
	apply := fsMergeVolumesCommand.Bool("apply", false, "applying the metadata changes")
	if err = fsMergeVolumesCommand.Parse(args); err != nil {
		return err
	}

	dir := *dirArg
	if dir != "/" {
		dir = strings.TrimRight(dir, "/")
	}

	fromVolumeId := needle.VolumeId(*fromVolumeArg)
	toVolumeId := needle.VolumeId(*toVolumeArg)

	c.reloadVolumesInfo(commandEnv.MasterClient)

	if fromVolumeId != 0 && toVolumeId != 0 {
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
		fromSize := c.getVolumeSizeById(fromVolumeId)
		toSize := c.getVolumeSizeById(toVolumeId)
		if fromSize+toSize > c.volumeSizeLimit {
			return fmt.Errorf(
				"volume %d (%d MB) cannot merge into volume %d (%d MB_ due to volume size limit (%d MB)",
				fromVolumeId, fromSize/1024/1024,
				toVolumeId, toSize/1024/1024,
				c.volumeSizeLimit/1024/102,
			)
		}
	}

	plan, err := c.createMergePlan(*collectionArg, toVolumeId, fromVolumeId)

	if err != nil {
		return err
	}
	c.printPlan(plan)

	if len(plan) == 0 {
		return nil
	}

	defer util_http.GetGlobalHttpClient().CloseIdleConnections()

	return commandEnv.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		return filer_pb.TraverseBfs(commandEnv, util.FullPath(dir), func(parentPath util.FullPath, entry *filer_pb.Entry) {
			if entry.IsDirectory {
				return
			}
			for _, chunk := range entry.Chunks {
				chunkVolumeId := needle.VolumeId(chunk.Fid.VolumeId)
				toVolumeId, found := plan[chunkVolumeId]
				if !found {
					continue
				}
				if chunk.IsChunkManifest {
					fmt.Printf("Change volume id for large file is not implemented yet: %s/%s\n", parentPath, entry.Name)
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

		c.volumeSizeLimit = volumes.GetVolumeSizeLimitMb() * 1024 * 1024

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

func (c *commandFsMergeVolumes) createMergePlan(collection string, toVolumeId needle.VolumeId, fromVolumeId needle.VolumeId) (map[needle.VolumeId]needle.VolumeId, error) {
	plan := make(map[needle.VolumeId]needle.VolumeId)
	volumeIds := maps.Keys(c.volumes)
	sort.Slice(volumeIds, func(a, b int) bool {
		return c.volumes[volumeIds[b]].Size < c.volumes[volumeIds[a]].Size
	})

	l := len(volumeIds)
	for i := 0; i < l; i++ {
		volume := c.volumes[volumeIds[i]]
		if volume.GetReadOnly() || c.getVolumeSize(volume) == 0 || (collection != "*" && collection != volume.GetCollection()) {

			if fromVolumeId != 0 && volumeIds[i] == fromVolumeId || toVolumeId != 0 && volumeIds[i] == toVolumeId {
				if volume.GetReadOnly() {
					return nil, fmt.Errorf("volume %d is readonly", volumeIds[i])
				}
				if c.getVolumeSize(volume) == 0 {
					return nil, fmt.Errorf("volume %d is empty", volumeIds[i])
				}
			}
			volumeIds = slices.Delete(volumeIds, i, i+1)
			i--
			l--
		}
	}
	for i := l - 1; i >= 0; i-- {
		src := volumeIds[i]
		if fromVolumeId != 0 && src != fromVolumeId {
			continue
		}
		for j := 0; j < i; j++ {
			candidate := volumeIds[j]
			if toVolumeId != 0 && candidate != toVolumeId {
				continue
			}
			if _, moving := plan[candidate]; moving {
				continue
			}
			compatible, err := c.volumesAreCompatible(src, candidate)
			if err != nil {
				return nil, err
			}
			if !compatible {
				fmt.Printf("volume %d is not compatible with volume %d\n", src, candidate)
				continue
			}
			if c.getVolumeSizeBasedOnPlan(plan, candidate)+c.getVolumeSizeById(src) > c.volumeSizeLimit {
				fmt.Printf("volume %d (%d MB) merge into volume %d (%d MB) exceeds volume size limit (%d MB)\n",
					src, c.getVolumeSizeById(src)/1024/1024,
					candidate, c.getVolumeSizeById(candidate)/1024/1024,
					c.volumeSizeLimit/1024/1024)
				continue
			}
			plan[src] = candidate
			break
		}
	}

	return plan, nil
}

func (c *commandFsMergeVolumes) getVolumeSizeBasedOnPlan(plan map[needle.VolumeId]needle.VolumeId, vid needle.VolumeId) uint64 {
	size := c.getVolumeSizeById(vid)
	for src, dest := range plan {
		if dest == vid {
			size += c.getVolumeSizeById(src)
		}
	}
	return size
}

func (c *commandFsMergeVolumes) getVolumeSize(volume *master_pb.VolumeInformationMessage) uint64 {
	return volume.Size - volume.DeletedByteCount
}

func (c *commandFsMergeVolumes) getVolumeSizeById(vid needle.VolumeId) uint64 {
	return c.getVolumeSize(c.volumes[vid])
}

func (c *commandFsMergeVolumes) printPlan(plan map[needle.VolumeId]needle.VolumeId) {
	fmt.Printf("max volume size: %d MB\n", c.volumeSizeLimit/1024/1024)
	reversePlan := make(map[needle.VolumeId][]needle.VolumeId)
	for src, dest := range plan {
		reversePlan[dest] = append(reversePlan[dest], src)
	}
	for dest, srcs := range reversePlan {
		currentSize := c.getVolumeSizeById(dest)
		for _, src := range srcs {
			srcSize := c.getVolumeSizeById(src)
			newSize := currentSize + srcSize
			fmt.Printf(
				"volume %d (%d MB) merge into volume %d (%d MB => %d MB)\n",
				src, srcSize/1024/1024,
				dest, currentSize/1024/1024, newSize/1024/1024,
			)
			currentSize = newSize

		}
		fmt.Println()
	}
}

func moveChunk(chunk *filer_pb.FileChunk, toVolumeId needle.VolumeId, masterClient *wdclient.MasterClient) error {
	fromFid := needle.NewFileId(needle.VolumeId(chunk.Fid.VolumeId), chunk.Fid.FileKey, chunk.Fid.Cookie)
	toFid := needle.NewFileId(toVolumeId, chunk.Fid.FileKey, chunk.Fid.Cookie)

	downloadURLs, err := masterClient.LookupVolumeServerUrl(fromFid.VolumeId.String())
	if err != nil {
		return err
	}

	downloadURL := fmt.Sprintf("http://%s/%s?readDeleted=true", downloadURLs[0], fromFid.String())

	uploadURLs, err := masterClient.LookupVolumeServerUrl(toVolumeId.String())
	if err != nil {
		return err
	}
	uploadURL := fmt.Sprintf("http://%s/%s", uploadURLs[0], toFid.String())

	resp, reader, err := readUrl(downloadURL)
	if err != nil {
		return err
	}
	defer util_http.CloseResponse(resp)
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

	uploader, err := operation.NewUploader()
	if err != nil {
		return err
	}

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	var jwt security.EncodedJwt
	if signingKey != "" {
		expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
		jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, toFid.String())
	}

	_, err, _ = uploader.Upload(reader, &operation.UploadOption{
		UploadUrl:         uploadURL,
		Filename:          filename,
		IsInputCompressed: isCompressed,
		Cipher:            false,
		MimeType:          contentType,
		PairMap:           nil,
		Md5:               md5,
		Jwt:               security.EncodedJwt(jwt),
	})
	if err != nil {
		return err
	}
	chunk.Fid.VolumeId = uint32(toVolumeId)
	chunk.FileId = ""

	return nil
}

func readUrl(fileUrl string) (*http.Response, io.ReadCloser, error) {

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Accept-Encoding", "gzip")

	r, err := util_http.GetGlobalHttpClient().Do(req)
	if err != nil {
		return nil, nil, err
	}
	if r.StatusCode >= 400 {
		util_http.CloseResponse(r)
		return nil, nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	return r, r.Body, nil
}
