package udm

import (
	"context"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/seaweedfs/seaweedfs/weed/storage/backend/udm/api/private/v1"
)

type ClientSet struct {
	conn *grpc.ClientConn

	tapeIOClient pb.TapeIOClient
}

func NewClient(target string) (*ClientSet, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &ClientSet{
		conn:         conn,
		tapeIOClient: pb.NewTapeIOClient(conn),
	}, nil
}

func (cs *ClientSet) Close() error {
	if cs.conn != nil {
		return cs.conn.Close()
	}
	return nil
}

func (cs *ClientSet) DownloadFile(ctx context.Context, volumeShortName, key string) error {
	_, err := cs.tapeIOClient.ReadFromTape(ctx, &pb.ReadFromTapeRequest{
		Files: []*pb.ReadFromTapeFileInfo{
			{
				Id: key,
				WriteTo: &pb.DataLocation{
					Host:         os.Getenv("POD_IP"),
					LocationType: pb.LocationType_volumeInternalCache,
					SubPath:      volumeShortName,
				},
			},
		},
	})

	return err
}
