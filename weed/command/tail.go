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
	cmdTail.Run = runTail // break init cycle
}

var cmdTail = &Command{
	UsageLine: "tail <wip> [-filer=localhost:8888]",
	Short:     "see recent changes on a filer",
	Long: `See recent changes on a filer.

  `,
}

var (
	tailFiler  = cmdTail.Flag.String("filer", "localhost:8888", "filer hostname:port")
	tailTarget = cmdTail.Flag.String("target", "/", "a folder or file on filer")
)

func runTail(cmd *Command, args []string) bool {

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	tailErr := pb.WithFilerClient(*tailFiler, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		stream, err := client.ListenForEvents(context.Background(), &filer_pb.ListenForEventsRequest{
			ClientName: "tail",
			Directory:  *tailTarget,
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
	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}

	return true
}
