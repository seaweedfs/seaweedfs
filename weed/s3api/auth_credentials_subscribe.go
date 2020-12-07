package s3api

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"time"
)

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, prefix string, lastTsNs int64) error {

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {

		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}

		dir := resp.Directory

		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		if dir == filer.IamConfigDirecotry && message.NewEntry.Name == filer.IamIdentityFile {
			if err := s3a.iam.loadS3ApiConfigurationFromFiler(s3a.option); err != nil {
				return err
			}
		}

		return nil
	}

	for {
		err := s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
				ClientName: clientName,
				PathPrefix: prefix,
				SinceNs:    lastTsNs,
			})
			if err != nil {
				return fmt.Errorf("subscribe: %v", err)
			}

			for {
				resp, listenErr := stream.Recv()
				if listenErr == io.EOF {
					return nil
				}
				if listenErr != nil {
					return listenErr
				}

				if err := processEventFn(resp); err != nil {
					glog.Fatalf("process %v: %v", resp, err)
				}
				lastTsNs = resp.TsNs
			}
		})
		if err != nil {
			glog.Errorf("subscribing filer meta change: %v", err)
		}
		time.Sleep(time.Second)
	}
}
