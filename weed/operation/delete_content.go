package operation

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"net/http"
	"strings"
	"sync"

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
func DeleteFileIds(masterFn GetMasterFn, usePublicUrl bool, grpcDialOption grpc.DialOption, fileIds []string) ([]*volume_server_pb.DeleteResult, error) {

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

func DeleteFileIdsWithLookupVolumeId(grpcDialOption grpc.DialOption, fileIds []string, lookupFunc func(vid []string) (map[string]*LookupResult, error)) ([]*volume_server_pb.DeleteResult, error) {

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
		return ret, err
	}

	server_to_fileIds := make(map[pb.ServerAddress][]string)
	for vid, result := range lookupResults {
		if result.Error != "" {
			ret = append(ret, &volume_server_pb.DeleteResult{
				FileId: vid,
				Status: http.StatusBadRequest,
				Error:  result.Error},
			)
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

			if deleteResults, deleteErr := DeleteFileIdsAtOneVolumeServer(server, grpcDialOption, fidList, false); deleteErr != nil {
				err = deleteErr
			} else if deleteResults != nil {
				resultChan <- deleteResults
			}

		}(server, fidList)
	}
	wg.Wait()
	close(resultChan)

	for result := range resultChan {
		ret = append(ret, result...)
	}

	return ret, err
}

// DeleteFileIdsAtOneVolumeServer deletes a list of files that is on one volume server via gRpc
func DeleteFileIdsAtOneVolumeServer(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fileIds []string, includeCookie bool) (ret []*volume_server_pb.DeleteResult, err error) {

	err = WithVolumeServerClient(false, volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

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
		return
	}

	for _, result := range ret {
		if result.Error != "" && result.Error != "not found" {
			return nil, fmt.Errorf("delete fileId %s: %v", result.FileId, result.Error)
		}
	}

	return

}
