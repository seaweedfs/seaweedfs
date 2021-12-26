package broker

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"io"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (broker *MessageBroker) appendToFile(targetFile string, topicConfig *messaging_pb.TopicConfiguration, data []byte) error {

	assignResult, uploadResult, err2 := broker.assignAndUpload(topicConfig, data)
	if err2 != nil {
		return err2
	}

	dir, name := util.FullPath(targetFile).DirAndName()

	// append the chunk
	if err := broker.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AppendToEntryRequest{
			Directory: dir,
			EntryName: name,
			Chunks:    []*filer_pb.FileChunk{uploadResult.ToPbFileChunk(assignResult.Fid, 0)},
		}

		_, err := client.AppendToEntry(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("append to file %v: %v", request, err)
			return err
		}

		return nil
	}); err != nil {
		return fmt.Errorf("append to file %v: %v", targetFile, err)
	}

	return nil
}

func (broker *MessageBroker) assignAndUpload(topicConfig *messaging_pb.TopicConfiguration, data []byte) (*operation.AssignResult, *operation.UploadResult, error) {

	var assignResult = &operation.AssignResult{}

	// assign a volume location
	if err := broker.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		assignErr := util.Retry("assignVolume", func() error {
			request := &filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: topicConfig.Replication,
				Collection:  topicConfig.Collection,
			}

			resp, err := client.AssignVolume(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("assign volume failure %v: %v", request, err)
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
			}

			assignResult.Auth = security.EncodedJwt(resp.Auth)
			assignResult.Fid = resp.FileId
			assignResult.Url = resp.Location.Url
			assignResult.PublicUrl = resp.Location.PublicUrl
			assignResult.GrpcPort = int(resp.Location.GrpcPort)
			assignResult.Count = uint64(resp.Count)

			return nil
		})
		if assignErr != nil {
			return assignErr
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}

	// upload data
	targetUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)
	uploadOption := &operation.UploadOption{
		UploadUrl:         targetUrl,
		Filename:          "",
		Cipher:            broker.option.Cipher,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               assignResult.Auth,
	}
	uploadResult, err := operation.UploadData(data, uploadOption)
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}
	// println("uploaded to", targetUrl)
	return assignResult, uploadResult, nil
}

var _ = filer_pb.FilerClient(&MessageBroker{})

func (broker *MessageBroker) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) (err error) {

	for _, filer := range broker.option.Filers {
		if err = pb.WithFilerClient(streamingMode, filer, broker.grpcDialOption, fn); err != nil {
			if err == io.EOF {
				return
			}
			glog.V(0).Infof("fail to connect to %s: %v", filer, err)
		} else {
			break
		}
	}

	return

}

func (broker *MessageBroker) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}
