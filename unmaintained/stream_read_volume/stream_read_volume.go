package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
)

var (
	volumeServer   = flag.String("volumeServer", "localhost:8080", "a volume server")
	volumeId       = flag.Int("volumeId", -1, "a volume id to stream read")
	grpcDialOption grpc.DialOption
)

func main() {
	flag.Parse()

	util.LoadConfiguration("security", false)
	grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	vid := uint32(*volumeId)

	eachNeedleFunc := func(resp *volume_server_pb.ReadAllNeedlesResponse) error {
		fmt.Printf("%d,%x%08x %d\n", resp.VolumeId, resp.NeedleId, resp.Cookie, len(resp.NeedleBlob))
		return nil
	}

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(*volumeServer), grpcDialOption, func(vs volume_server_pb.VolumeServerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		copyFileClient, err := vs.ReadAllNeedles(ctx, &volume_server_pb.ReadAllNeedlesRequest{
			VolumeIds: []uint32{vid},
		})
		if err != nil {
			return err
		}
		for {
			resp, err := copyFileClient.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err = eachNeedleFunc(resp); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("read %s: %v\n", *volumeServer, err)
	}

}
