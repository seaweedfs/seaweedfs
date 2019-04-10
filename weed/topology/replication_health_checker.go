package topology

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"google.golang.org/grpc"
	"sort"
	"strings"
	"sync"
)

/**
	check the replication health
 */
func (t *Topology) RepairUnhealthyReplicationInLayout(grpcDialOption grpc.DialOption, layout *VolumeLayout, eVid storage.VolumeId) error {
	ctx := context.Background()
	locations, exist := layout.vid2location[eVid]
	if !exist {
		retErr := fmt.Errorf("the volume:%v has no locations", eVid)
		glog.V(0).Infof(retErr.Error())
		return retErr
	}

	//glog.V(5).Infof("volume:%v, locations:%v", eVid, locations.list)
	fileStat, err := getReplicationInfo(grpcDialOption, ctx, eVid, locations)
	if err != nil {
		glog.Errorf("get replication status failed, %v", err)
		return err
	}

	if isSameVolumeReplications(fileStat, layout.volumeSizeLimit) {
		glog.V(0).Infof("the volume:%v has %d same replication, need not repair", eVid, len(fileStat))
		return nil
	}

	// compact all the replications of volume
	{
		glog.V(4).Infof("begin compact all the replications of volume:%v", eVid)
		allUrls := make([]string, 0, len(fileStat))
		for _, fs := range fileStat {
			allUrls = append(allUrls, fs.url)
		}

		if tryBatchCompactVolume(ctx, grpcDialOption, eVid, allUrls) == false {
			err := fmt.Errorf("compact all the replications of volume:%v", eVid)
			glog.Error(err.Error())
			return err
		}
		glog.V(4).Infof("success compact all the replications of volume:%v", eVid)
	}

	// get replication status again
	fileStat, err = getReplicationInfo(grpcDialOption, ctx, eVid, locations)
	if err != nil {
		return err
	}

	okUrls, errUrls := filterErrorReplication(fileStat)
	if len(errUrls) == 0 {
		return nil // they are the same
	}

	if len(okUrls) == 0 {
		return fmt.Errorf("no correct volume replications, that's impossible")
	}

	glog.V(4).Infof("need repair replication : %v", errUrls)
	if len(locations.list) <= 0 {
		return fmt.Errorf("that's impossible, the locatins of volume:%v is empty", eVid)
	}
	for _, url := range errUrls {
		vInfo := locations.list[0].volumes[eVid]
		err = syncReplication(grpcDialOption, okUrls[0], url, vInfo)
		if nil != err {
			glog.Error(err)
			return err
		}
	}
	return nil
}

type FileStatus struct {
	url      string
	fileStat *volume_server_pb.ReadVolumeFileStatusResponse
}

func getReplicationInfo(grpcDialOption grpc.DialOption, ctx context.Context, vid storage.VolumeId, locs *VolumeLocationList) (fs []FileStatus, err error) {
	type ResponsePair struct {
		url    string
		status *volume_server_pb.ReadVolumeFileStatusResponse
		err    error
	}

	var wg sync.WaitGroup
	resultChan := make(chan ResponsePair, len(locs.list))
	wg.Add(len(locs.list))
	getFileStatFunc := func(url string, volumeId storage.VolumeId) {
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
	for _, node := range locs.list {
		go getFileStatFunc(node.Url(), vid)
	}

	go func() { // close channel
		wg.Wait()
		close(resultChan)
	}()

	var errs []string
	for result := range resultChan {
		if result.err == nil {
			fs = append(fs, FileStatus{
				url:      result.url,
				fileStat: result.status,
			})
			continue
		}
		tmp := fmt.Sprintf("url : %s, error : %v", result.url, result.err)
		errs = append(errs, tmp)
	}

	if len(fs) == len(locs.list) {
		return fs, nil
	}
	err = fmt.Errorf("get volume[%v] replication status failed, err : %s", vid, strings.Join(errs, "; "))
	return nil, err
}

/**
<see the class mapMetric and needleMap> :
	the file count is the total count of the volume received from user clients
todo: this policy is not perfected or not rigorous, need fix
 */
func filterErrorReplication(fileStat []FileStatus) (okUrls, errUrls []string) {
	sort.Slice(fileStat, func(i, j int) bool {
		return fileStat[i].fileStat.FileCount > fileStat[j].fileStat.FileCount
	})
	if fileStat[0].fileStat.FileCount != fileStat[len(fileStat)-1].fileStat.FileCount {
		okFileCounter := fileStat[0].fileStat.FileCount
		for _, v := range fileStat {
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
func compactVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid storage.VolumeId) bool {
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
func commitCompactedVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid storage.VolumeId) bool {
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
func cleanupCompactedVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeUrl string, vid storage.VolumeId) bool {
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

func tryCompactVolume(ctx context.Context, grpcDialOption grpc.DialOption, vid storage.VolumeId, volumeUrl string) bool {
	if compactVolume(ctx, grpcDialOption, volumeUrl, vid) == false {
		return cleanupCompactedVolume(ctx, grpcDialOption, volumeUrl, vid)
	}
	return commitCompactedVolume(ctx, grpcDialOption, volumeUrl, vid)
}

func tryBatchCompactVolume(ctx context.Context, grpcDialOption grpc.DialOption,
	vid storage.VolumeId, urls []string) bool {
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

func isSameVolumeReplications(fileStat []FileStatus, volumeSizeLimit uint64) bool {
	fileSizeSet := make(map[uint64]bool)
	fileCountSet := make(map[uint64]bool)
	lastModifiedSet := make(map[uint64]bool)
	var oneFileSize uint64 = 0
	for _, v := range fileStat {
		fileCountSet[v.fileStat.FileCount] = true
		lastModifiedSet[v.fileStat.DatFileTimestamp] = true
		fileSizeSet[v.fileStat.DatFileSize] = true
		oneFileSize = v.fileStat.DatFileSize
	}

	if (len(lastModifiedSet) == 1) && (len(fileCountSet) == 1) &&
		(len(fileSizeSet) == 1) && (oneFileSize >= volumeSizeLimit) {
		return true
	}
	return false
}

func syncReplication(grpcDialOption grpc.DialOption, srcUrl, destUrl string, vinfo storage.VolumeInfo) error {
	ctx := context.Background()
	err := operation.WithVolumeServerClient(destUrl, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if _, err := client.ReplicateVolume(ctx, &volume_server_pb.ReplicateVolumeRequest{
				VolumeId:       uint32(vinfo.Id),
				Collection:     vinfo.Collection,
				Replication:    vinfo.ReplicaPlacement.String(),
				Ttl:            vinfo.Ttl.String(),
				SourceDataNode: srcUrl,
			}); err != nil {
				glog.Errorf("sync replication failed, %v", err)
				return err
			}
			return nil
		})
	return err
}
