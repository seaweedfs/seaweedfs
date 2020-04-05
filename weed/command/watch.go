package command

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	cmdWatch.Run = runWatch // break init cycle
}

var cmdWatch = &Command{
	UsageLine: "watch <wip> [-filer=localhost:8888] [-target=/]",
	Short:     "see recent changes on a filer",
	Long: `See recent changes on a filer.

  `,
}

var (
	watchFiler  = cmdWatch.Flag.String("filer", "localhost:8888", "filer hostname:port")
	watchTarget = cmdWatch.Flag.String("target", "/", "a folder or file on filer")
)

func runWatch(cmd *Command, args []string) bool {

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	watchErr := pb.WithFilerClient(*watchFiler, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		stream, err := client.ListenForEvents(context.Background(), &filer_pb.ListenForEventsRequest{
			ClientName: "watch",
			Directory:  *watchTarget,
			SinceNs:    0,
		})
		if err != nil {
			return fmt.Errorf("listen: %v", err)
		}

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				return listenErr
			}
			fmt.Printf("events: %+v\n", resp.EventNotification)
		}

	})
	if watchErr != nil {
		fmt.Printf("watch %s: %v\n", *watchFiler, watchErr)
	}

	return true
}
