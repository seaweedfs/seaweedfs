package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
)

func init() {
	//commands = append(commands, &commandReplicationHealthChecker{})
	//commands = append(commands, &commandReplicationHealthRepair{})
}

type commandReplicationHealthChecker struct {
	args       []string
	commandEnv *commandEnv
	writer     io.Writer
	checker    *ReplicationHealthChecker
}

func (c *commandReplicationHealthChecker) Name() string {
	return "volume.check.replication"
}

func (c *commandReplicationHealthChecker) Help() string {
	return `
`
}

func (c *commandReplicationHealthChecker) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {
	c.writer = writer
	c.commandEnv = commandEnv
	c.args = args
	c.checker = NewReplicationHealthChecker(context.Background(), commandEnv.option.GrpcDialOption)
	return nil
}
type commandReplicationHealthRepair struct {
	args       []string
	commandEnv *commandEnv
	writer     io.Writer
	repair     *ReplicationHealthRepair
}

func (c *commandReplicationHealthRepair) Name() string {
	return "volume.repair.replication"
}

func (c *commandReplicationHealthRepair) Help() string {
	return `
`
}

func (c *commandReplicationHealthRepair) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {
	c.args = args
	c.commandEnv = commandEnv
	c.writer = writer
	ctx := context.Background()
	c.repair = NewReplicationHealthRepair(ctx, commandEnv.option.GrpcDialOption)

	var resp *master_pb.VolumeListResponse
	if err := c.commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		var err error
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	}); err != nil {
		return err
	}

	// invocation the commandReplicationHealthChecker and get the error replications
	checker := NewReplicationHealthChecker(ctx, c.commandEnv.option.GrpcDialOption)
	eVids, err := checker.Check(resp.TopologyInfo)
	if err != nil {
		writer.Write([]byte(err.Error()))
		return err
	}

	// repair them
	successVids, failedVids, err := c.repair.Repair(resp.TopologyInfo, eVids)
	if err != nil {
		str := fmt.Sprintf("repair volume:%v replication failed.\n", failedVids)
		writer.Write([]byte(str))
	} else {
		str := fmt.Sprintf("repair volue:%v replication success.\n", successVids)
		writer.Write([]byte(str))
	}
	return nil
}

/////////////////////////////////////////////////////////////////////////
type ReplicationHealthChecker struct {
	grpcDialOption grpc.DialOption
	context        context.Context
}

func NewReplicationHealthChecker(ctx context.Context, grpcOption grpc.DialOption) *ReplicationHealthChecker {
	return &ReplicationHealthChecker{grpcDialOption: grpcOption, context: ctx}
}

/**
double check :
	1st, get information of volume from topology;
	2nd, get the latest information of volume from every data node
 */
func (r *ReplicationHealthChecker) Check(topologyInfo *master_pb.TopologyInfo) ([]uint32, error) {
	volInfoMap, vol2LocsMap := getVolumeInfo(topologyInfo)
	if (nil == volInfoMap) || (nil == vol2LocsMap) || (len(volInfoMap) <= 0) || (len(vol2LocsMap) <= 0) {
		return nil, fmt.Errorf("get volume info from topology failed")
	}

	errVids := getUnhealthyVolumeIds(volInfoMap, vol2LocsMap, topologyInfo.VolumeSizeLimitBytes)
	if nil == errVids || (len(errVids) <= 0) {
		glog.V(4).Infof("no error replications")
		return nil, nil
	}

	// get the latest volume file status from every data node
	newErrVids := make([]uint32, 0, len(errVids))
	for _, eVid := range errVids {
		eVidUrls := getVolumeUrls(vol2LocsMap[eVid])
		fileStats, err := getVolumeFileStatus(r.grpcDialOption, r.context, eVid, eVidUrls)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		vInfos := make([]*ReplicaInformation, 0, len(fileStats))
		for _, i := range fileStats {
			vInfos = append(vInfos, &ReplicaInformation{
				Size:                   i.fileStat.Size,
				FileCount:              i.fileStat.FileCount,
				ReadOnly:               i.fileStat.ReadOnly,
				CompactRevision:        i.fileStat.CompactRevision,
				LastCompactIndexOffset: i.fileStat.LastCompactIndexOffset,
			})
		}
		if isHealthyVolumeReplications(vInfos, topologyInfo.VolumeSizeLimitBytes) {
			continue
		}
		newErrVids = append(newErrVids, eVid)
	}
	return newErrVids, nil
}

/////////////////////////////////////////////////////////////////////////
type ReplicationHealthRepair struct {
	grpcDialOption grpc.DialOption
	context        context.Context
}

func NewReplicationHealthRepair(ctx context.Context, grpcOption grpc.DialOption) *ReplicationHealthRepair {
	return &ReplicationHealthRepair{grpcDialOption: grpcOption, context: ctx,}
}

/**
	repair the unhealthy replications,
 */
func (r *ReplicationHealthRepair) Repair(topologyInfo *master_pb.TopologyInfo, errVids []uint32) (success, failed []uint32, err error) {
	volInfoMap, vol2LocsMap := getVolumeInfo(topologyInfo)
	if (nil == volInfoMap) || (nil == vol2LocsMap) || (len(volInfoMap) <= 0) || (len(vol2LocsMap) <= 0) {
		return nil, errVids, fmt.Errorf("get volume info from topology failed")
	}

	for _, eVid := range errVids {
		if isReadOnlyVolume(volInfoMap[eVid]) {
			continue  // skip the read-only volume compacting
		}
		glog.V(4).Infof("begin compact all the replications of volume:%v", eVid)
		eVidUrls := getVolumeUrls(vol2LocsMap[eVid])
		if tryBatchCompactVolume(r.context, r.grpcDialOption, needle.VolumeId(eVid), eVidUrls) == false {
			err := fmt.Errorf("compact all the replications of volume:%v", eVid)
			glog.Error(err)
			return nil, errVids, err
		}
		glog.V(4).Infof("success compact all the replications of volume:%v", eVid)
	}

	for _, eVid := range errVids {
		eVidUrls := getVolumeUrls(vol2LocsMap[eVid])
		fileStats, err := getVolumeFileStatus(r.grpcDialOption, r.context, eVid, eVidUrls)
		if err != nil {
			glog.Error(err)
			failed = append(failed, eVid)
			continue
		}
		okUrls, errUrls := filterErrorReplication(fileStats)
		if len(errUrls) == 0 {
			success = append(success, eVid) // no need repair
			continue
		}

		info := volInfoMap[eVid][0]
		ttl := needle.LoadTTLFromUint32(info.Ttl).String()
		rp, err := storage.NewReplicaPlacementFromByte(byte(info.ReplicaPlacement))
		if err != nil {
			failed = append(failed, eVid)
			glog.Errorf("vid:%v, parse replicaPlacement failed, %d", eVid, info.ReplicaPlacement)
			continue
		}

		syncSuccess := true
		for _, errUrl := range errUrls {
			okUrl := okUrls[rand.Intn(len(okUrls))]
			req := &volume_server_pb.VolumeCopyRequest{
				VolumeId:       uint32(info.Id),
				Collection:     info.Collection,
				Replication:    rp.String(),
				Ttl:            ttl,
				SourceDataNode: okUrl,
			}
			err = syncReplication(r.grpcDialOption, errUrl, req)
			if nil != err {
				syncSuccess = false
				glog.Errorf("sync replication from %s to %s failed, %v", okUrl, errUrl, err)
			}
		}
		if syncSuccess {
			success = append(success, eVid)
		} else {
			failed = append(failed, eVid)
		}
	}

	if len(failed) > 0 {
		err = fmt.Errorf("there are some volumes health repair failed")
	}
	return
}

type ReplicaFileStatus struct {
	url      string
	fileStat *ReplicaInformation
}

/**
 get information of volume from every volume node concurrently
 */
func getVolumeFileStatus(grpcDialOption grpc.DialOption, ctx context.Context, vid uint32, volumeUrls []string) (fileStatuses []*ReplicaFileStatus, err error) {
	type ResponsePair struct {
		url    string
		status *volume_server_pb.ReadVolumeFileStatusResponse
		err    error
	}

	var wg sync.WaitGroup
	resultChan := make(chan ResponsePair, len(volumeUrls))
	wg.Add(len(volumeUrls))
	getFileStatFunc := func(url string, volumeId uint32) {
		defer wg.Done()
		glog.V(4).Infof("volumeId:%v, location:%v", volumeId, url)
		err := operation.WithVolumeServerClient(url, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			req := &volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: uint32(volumeId),
			}
			respTmp, err := client.ReadVolumeFileStatus(ctx, req)
			resultChan <- ResponsePair{
				url:    url,
				status: respTmp,
				err:    err,
			}
			return nil
		})
		if nil != err {
			glog.Error(err)
		}
	}
	for _, url := range volumeUrls {
		go getFileStatFunc(url, vid)
	}

	go func() { // close channel
		wg.Wait()
		close(resultChan)
	}()

	var errs []string
	for result := range resultChan {
		if result.err == nil {
			fileStatuses = append(fileStatuses, &ReplicaFileStatus{
				url: result.url,
				fileStat: &ReplicaInformation{
					Size:                   result.status.DatFileSize,
					FileCount:              result.status.FileCount,
					ReadOnly:               false,
					CompactRevision:        0,
					LastCompactIndexOffset: 0,
				}})
			continue
		}
		tmp := fmt.Sprintf("url : %s, error : %v", result.url, result.err)
		errs = append(errs, tmp)
	}

	if len(fileStatuses) == len(volumeUrls) {
		return fileStatuses, nil
	}
	err = fmt.Errorf("get volume[%v] replication status failed, err : %s", vid, strings.Join(errs, "; "))
	return nil, err
}

/**
<see the class mapMetric and needleMap> :
	the file count is the total count of the volume received from user clients
 */
func filterErrorReplication(vInfo []*ReplicaFileStatus) (okUrls, errUrls []string) {
	sort.Slice(vInfo, func(i, j int) bool {
		return vInfo[i].fileStat.FileCount > vInfo[j].fileStat.FileCount
	})
	if vInfo[0].fileStat.FileCount != vInfo[len(vInfo)-1].fileStat.FileCount {
		okFileCounter := vInfo[0].fileStat.FileCount
		for _, v := range vInfo {
			if okFileCounter == v.fileStat.FileCount {
				okUrls = append(okUrls, v.url)
			} else {
				errUrls = append(errUrls, v.url)
			}
		}
		return
	}
	return
}

// execute the compact transaction
func compactVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid needle.VolumeId) bool {
	glog.V(0).Infoln("Start vacuuming", vid, "on", volumeUrl)
	err := operation.WithVolumeServerClient(volumeUrl, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, err := volumeServerClient.VacuumVolumeCompact(ctx, &volume_server_pb.VacuumVolumeCompactRequest{
			VolumeId: uint32(vid),
		})
		return err
	})
	if err != nil {
		glog.Errorf("Error when vacuuming %d on %s: %v", vid, volumeUrl, err)
		return false
	}
	glog.V(0).Infof("Complete vacuuming volume:%v on %s", vid, volumeUrl)
	return true
}

// commit the compact transaction when compactVolume() return true
func commitCompactedVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid needle.VolumeId) bool {
	err := operation.WithVolumeServerClient(volumeUrl, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, err := volumeServerClient.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{
			VolumeId: uint32(vid),
		})
		return err
	})
	if err != nil {
		glog.Errorf("Error when committing vacuum %d on %s: %v", vid, volumeUrl, err)
		return false
	}
	glog.V(0).Infof("Complete Committing vacuum %d on %s", vid, volumeUrl)
	return true
}

// rollback the compact transaction when compactVolume return false
func cleanupCompactedVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid needle.VolumeId) bool {
	glog.V(0).Infoln("Start cleaning up", vid, "on", volumeUrl)
	err := operation.WithVolumeServerClient(volumeUrl, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, err := volumeServerClient.VacuumVolumeCleanup(ctx, &volume_server_pb.VacuumVolumeCleanupRequest{
			VolumeId: uint32(vid),
		})
		return err
	})
	if err != nil {
		glog.Errorf("Error when cleaning up vacuum %d on %s: %v", vid, volumeUrl, err)
		return false
	}
	glog.V(0).Infof("Complete cleaning up vacuum %d on %s", vid, volumeUrl)
	return false
}

func tryCompactVolume(ctx context.Context, grpcDialOption grpc.DialOption, vid needle.VolumeId, volumeUrl string) bool {
	if compactVolume(ctx, grpcDialOption, volumeUrl, vid) == false {
		return cleanupCompactedVolume(ctx, grpcDialOption, volumeUrl, vid)
	}
	return commitCompactedVolume(ctx, grpcDialOption, volumeUrl, vid)
}

func tryBatchCompactVolume(ctx context.Context, grpcDialOption grpc.DialOption, vid needle.VolumeId, urls []string) bool {
	resultChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(urls))
	for _, url := range urls {
		go func(volumeUrl string) {
			defer wg.Done()
			if tryCompactVolume(ctx, grpcDialOption, vid, volumeUrl) == false {
				resultChan <- fmt.Errorf("url:%s", volumeUrl)
			}
		}(url)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var errs []string
	for result := range resultChan {
		if result != nil {
			errs = append(errs, result.Error())
		}
	}
	if len(errs) > 0 {
		glog.Errorf("consist volume:%v compact reversion failed, %s", vid, strings.Join(errs, "; "))
		return false
	}
	return true
}

func getVolumeUrls(locs []*master_pb.DataNodeInfo) []string {
	eVidUrls := make([]string, 0, len(locs))
	for _, loc := range locs {
		eVidUrls = append(eVidUrls, loc.Url)
	}
	return eVidUrls
}

type ReplicaInformation struct {
	Size                   uint64
	FileCount              uint64
	ReadOnly               bool
	CompactRevision        uint32
	LastCompactIndexOffset uint64
}

func getUnhealthyVolumeIds(volInfoMap map[uint32][]*master_pb.VolumeInformationMessage,
	vol2LocsMap map[uint32][]*master_pb.DataNodeInfo, volumeSizeLimitMB uint64) []uint32 {
	errVids := make([]uint32, 0, len(vol2LocsMap))
	for vid, info := range volInfoMap {
		vInfos := make([]*ReplicaInformation, 0, len(info))
		for _, i := range info {
			vInfos = append(vInfos, &ReplicaInformation{
				Size:            i.Size,
				FileCount:       i.FileCount,
				ReadOnly:        i.ReadOnly,
				CompactRevision: i.CompactRevision,
			})
		}
		if isHealthyVolumeReplications(vInfos, volumeSizeLimitMB*1024*1024) {
			glog.V(4).Infof("the volume:%v has %d same replication, need not repair", vid, len(info))
			continue
		}
		errVids = append(errVids, vid)
	}
	return errVids
}

func isHealthyVolumeReplications(volInfo []*ReplicaInformation, volumeSizeLimit uint64) bool {
	fileSizeSet := make(map[uint64]bool)
	fileCountSet := make(map[uint64]bool)
	compactVersionSet := make(map[uint32]bool)
	compactOffsetSet := make(map[uint64]bool)
	//lastModifiedSet := make(map[uint64]bool)
	var oneFileSize uint64 = 0
	for _, v := range volInfo {
		fileCountSet[v.FileCount] = true
		//lastModifiedSet[v.] = true
		fileSizeSet[v.Size] = true
		oneFileSize = v.Size
		compactVersionSet[v.CompactRevision] = true
		compactOffsetSet[v.LastCompactIndexOffset] = true
	}

	if (len(fileSizeSet) == 1) && (oneFileSize >= volumeSizeLimit) && (len(fileCountSet) == 1) {
		return true
	}

	if len(fileCountSet) != 1 {
		return false
	}
	if len(fileSizeSet) != 1 {
		return false
	}
	if len(compactVersionSet) != 1 {
		return false
	}
	//if len(compactOffsetSet) != 1 {
	//	return false
	//}
	return true
}

func isReadOnlyVolume(replicaInfo []*master_pb.VolumeInformationMessage) bool {
	readOnlySet := make(map[bool]bool)
	for _, info := range replicaInfo {
		readOnlySet[info.ReadOnly] = true
	}
	if _, exist := readOnlySet[true]; exist {
		return len(readOnlySet) == 1
	}
	return false
}

func syncReplication(grpcDialOption grpc.DialOption, destUrl string, req *volume_server_pb.VolumeCopyRequest) error {
	ctx := context.Background()
	err := operation.WithVolumeServerClient(destUrl, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if _, err := client.VolumeCopy(ctx, req); err != nil {
				glog.Errorf("sync replication failed, %v", err)
				return err
			}
			return nil
		})
	return err
}

func getVolumeInfo(topo *master_pb.TopologyInfo) (map[uint32][]*master_pb.VolumeInformationMessage, map[uint32][]*master_pb.DataNodeInfo) {
	volInfoMap := make(map[uint32][]*master_pb.VolumeInformationMessage)
	vol2LocsMap := make(map[uint32][]*master_pb.DataNodeInfo)
	IterateVolumes(topo, func(dc *master_pb.DataCenterInfo, rack *master_pb.RackInfo, dataNode *master_pb.DataNodeInfo, vol *master_pb.VolumeInformationMessage) {
		volInfoMap[vol.Id] = append(volInfoMap[vol.Id], vol)
		vol2LocsMap[vol.Id] = append(vol2LocsMap[vol.Id], dataNode)
	})

	return volInfoMap, vol2LocsMap
}

func IterateVolumes(topo *master_pb.TopologyInfo,
	callBack func(dc *master_pb.DataCenterInfo, rack *master_pb.RackInfo, dataNode *master_pb.DataNodeInfo, vol *master_pb.VolumeInformationMessage)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				for _, vol := range dn.VolumeInfos {
					callBack(dc, rack, dn, vol)
				}
			}
		}
	}
}
