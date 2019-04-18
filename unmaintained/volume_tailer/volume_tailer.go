package main

import (
	"context"
	"flag"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/spf13/viper"

	"io"
	"log"
)

var (
	master         = flag.String("master", "localhost:9333", "master server host and port")
	volumeId       = flag.Int("volumeId", -1, "a volume id")
	timeoutSeconds = flag.Int("timeoutSeconds", 0, "disconnect if no activity after these seconds")
)

func main() {
	flag.Parse()

	weed_server.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "client")

	vid := storage.VolumeId(*volumeId)

	// find volume location, replication, ttl info
	lookup, err := operation.Lookup(*master, vid.String())
	if err != nil {
		log.Printf("Error looking up volume %d: %v", vid, err)
		return
	}
	volumeServer := lookup.Locations[0].Url
	log.Printf("volume %d is on volume server %s", vid, volumeServer)

	err = operation.WithVolumeServerClient(volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		stream, err := client.VolumeTail(context.Background(), &volume_server_pb.VolumeTailRequest{
			VolumeId:        uint32(vid),
			SinceNs:         0,
			DrainingSeconds: uint32(*timeoutSeconds),
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

			println("header:", len(resp.NeedleHeader), "body:", len(resp.NeedleBody))
		}

		return nil

	})

	if err != nil {
		log.Printf("Error VolumeTail volume %d: %v", vid, err)
	}


}
