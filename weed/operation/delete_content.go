package operation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type DeleteResult struct {
	Fid    string `json:"fid"`
	Size   int    `json:"size"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

func ParseFileId(fid string) (vid string, key_cookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", errors.New("Wrong fid format.")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}

// DeleteFileIds batch deletes a list of fileIds
// Returns individual results for each file ID. Check result.Error for per-file failures.
func DeleteFileIds(masterFn GetMasterFn, usePublicUrl bool, grpcDialOption grpc.DialOption, fileIds []string) []*volume_server_pb.DeleteResult {

	lookupFunc := func(vids []string) (results map[string]*LookupResult, err error) {
		results, err = LookupVolumeIds(masterFn, grpcDialOption, vids)
		if err == nil && usePublicUrl {
			for _, result := range results {
				for _, loc := range result.Locations {
					loc.Url = loc.PublicUrl
				}
			}
		}
		return
	}

	return DeleteFileIdsWithLookupVolumeId(grpcDialOption, fileIds, lookupFunc)

}

func DeleteFileIdsWithLookupVolumeId(grpcDialOption grpc.DialOption, fileIds []string, lookupFunc func(vid []string) (map[string]*LookupResult, error)) []*volume_server_pb.DeleteResult {

	var ret []*volume_server_pb.DeleteResult

	vid_to_fileIds := make(map[string][]string)
	var vids []string
	for _, fileId := range fileIds {
		vid, _, err := ParseFileId(fileId)
		if err != nil {
			ret = append(ret, &volume_server_pb.DeleteResult{
				FileId: fileId,
				Status: http.StatusBadRequest,
				Error:  err.Error()},
			)
			continue
		}
		if _, ok := vid_to_fileIds[vid]; !ok {
			vid_to_fileIds[vid] = make([]string, 0)
			vids = append(vids, vid)
		}
		vid_to_fileIds[vid] = append(vid_to_fileIds[vid], fileId)
	}

	lookupResults, err := lookupFunc(vids)
	if err != nil {
		// Lookup failed - return error results for all file IDs that passed parsing
		for _, fids := range vid_to_fileIds {
			for _, fileId := range fids {
				ret = append(ret, &volume_server_pb.DeleteResult{
					FileId: fileId,
					Status: http.StatusInternalServerError,
					Error:  fmt.Sprintf("lookup error: %v", err),
				})
			}
		}
		return ret
	}

	server_to_fileIds := make(map[pb.ServerAddress][]string)
	for vid, result := range lookupResults {
		if result.Error != "" {
			// Lookup error for this volume - mark all its files as failed
			for _, fileId := range vid_to_fileIds[vid] {
				ret = append(ret, &volume_server_pb.DeleteResult{
					FileId: fileId,
					Status: http.StatusBadRequest,
					Error:  result.Error},
				)
			}
			continue
		}
		for _, location := range result.Locations {
			serverAddress := location.ServerAddress()
			if _, ok := server_to_fileIds[serverAddress]; !ok {
				server_to_fileIds[serverAddress] = make([]string, 0)
			}
			server_to_fileIds[serverAddress] = append(
				server_to_fileIds[serverAddress], vid_to_fileIds[vid]...)
		}
	}

	resultChan := make(chan []*volume_server_pb.DeleteResult, len(server_to_fileIds))
	var wg sync.WaitGroup
	for server, fidList := range server_to_fileIds {
		wg.Add(1)
		go func(server pb.ServerAddress, fidList []string) {
			defer wg.Done()

			resultChan <- DeleteFileIdsAtOneVolumeServer(server, grpcDialOption, fidList, false)

		}(server, fidList)
	}
	wg.Wait()
	close(resultChan)

	for result := range resultChan {
		ret = append(ret, result...)
	}

	return ret
}

// DeleteFileIdsAtOneVolumeServer deletes a list of files that is on one volume server via gRpc
// Returns individual results for each file ID. Check result.Error for per-file failures.
func DeleteFileIdsAtOneVolumeServer(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fileIds []string, includeCookie bool) []*volume_server_pb.DeleteResult {

	var ret []*volume_server_pb.DeleteResult

	err := WithVolumeServerClient(false, volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		req := &volume_server_pb.BatchDeleteRequest{
			FileIds:         fileIds,
			SkipCookieCheck: !includeCookie,
		}

		resp, err := volumeServerClient.BatchDelete(context.Background(), req)

		// fmt.Printf("deleted %v %v: %v\n", fileIds, err, resp)

		if err != nil {
			return err
		}

		ret = append(ret, resp.Results...)

		return nil
	})

	if err != nil {
		// Connection or communication error - return error results for all files
		ret = make([]*volume_server_pb.DeleteResult, 0, len(fileIds))
		for _, fileId := range fileIds {
			ret = append(ret, &volume_server_pb.DeleteResult{
				FileId: fileId,
				Status: http.StatusInternalServerError,
				Error:  err.Error(),
			})
		}
	}

	return ret

}
