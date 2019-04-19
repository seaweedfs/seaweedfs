package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

func TailVolume(master string, grpcDialOption grpc.DialOption, vid VolumeId, sinceNs uint64, timeoutSeconds int, fn func(n *Needle) error) error {
	// find volume location, replication, ttl info
	lookup, err := operation.Lookup(master, vid.String())
	if err != nil {
		return fmt.Errorf("look up volume %d: %v", vid, err)
	}
	if len(lookup.Locations) == 0 {
		return fmt.Errorf("unable to locate volume %d", vid)
	}

	volumeServer := lookup.Locations[0].Url

	return operation.WithVolumeServerClient(volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		stream, err := client.VolumeTail(context.Background(), &volume_server_pb.VolumeTailRequest{
			VolumeId:        uint32(vid),
			SinceNs:         sinceNs,
			DrainingSeconds: uint32(timeoutSeconds),
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

			n := new(Needle)
			n.ParseNeedleHeader(needleHeader)
			n.ReadNeedleBodyBytes(needleBody, CurrentVersion)

			err = fn(n)

			if err != nil {
				return err
			}

		}
		return nil
	})
}
