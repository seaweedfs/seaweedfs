package operation

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TailVolume(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vid needle.VolumeId, sinceNs uint64, timeoutSeconds int, fn func(n *needle.Needle) error) error {
	// find volume location, replication, ttl info
	lookup, err := LookupVolumeId(masterFn, grpcDialOption, vid.String())
	if err != nil {
		return fmt.Errorf("look up volume %d: %v", vid, err)
	}
	if len(lookup.Locations) == 0 {
		return fmt.Errorf("unable to locate volume %d", vid)
	}

	volumeServer := lookup.Locations[0].ServerAddress()

	return TailVolumeFromSource(volumeServer, grpcDialOption, vid, sinceNs, timeoutSeconds, fn)
}

func TailVolumeFromSource(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, vid needle.VolumeId, sinceNs uint64, idleTimeoutSeconds int, fn func(n *needle.Needle) error) error {
	return WithVolumeServerClient(true, volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.VolumeTailSender(ctx, &volume_server_pb.VolumeTailSenderRequest{
			VolumeId:           uint32(vid),
			SinceNs:            sinceNs,
			IdleTimeoutSeconds: uint32(idleTimeoutSeconds),
		})
		if err != nil {
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			needleHeader := resp.NeedleHeader
			needleBody := resp.NeedleBody

			if len(needleHeader) == 0 {
				continue
			}

			for !resp.IsLastChunk {
				resp, recvErr = stream.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					} else {
						return recvErr
					}
				}
				needleBody = append(needleBody, resp.NeedleBody...)
			}

			n := new(needle.Needle)
			n.ParseNeedleHeader(needleHeader)
			err = n.ReadNeedleBodyBytes(needleBody, needle.CurrentVersion)
			if err != nil {
				return err
			}

			err = fn(n)

			if err != nil {
				return err
			}

		}
		return nil
	})
}
