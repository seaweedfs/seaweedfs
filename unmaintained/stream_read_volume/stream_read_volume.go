package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	volumeServer   = flag.String("volumeServer", "localhost:8080", "a volume server")
	volumeId       = flag.Int("volumeId", -1, "a volume id to stream read")
	grpcDialOption grpc.DialOption
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	util.LoadSecurityConfiguration()
	grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	vid := uint32(*volumeId)

	eachNeedleFunc := func(resp *volume_server_pb.ReadAllNeedlesResponse) error {
		fmt.Printf("%d,%x%08x %d %v %d %x\n", resp.VolumeId, resp.NeedleId, resp.Cookie, len(resp.NeedleBlob), resp.NeedleBlobCompressed, resp.LastModified, resp.Crc)
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
