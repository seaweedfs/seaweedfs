package command

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	cmdWatch.Run = runWatch // break init cycle
}

var cmdWatch = &Command{
	UsageLine: "watch [-filer=localhost:8888] [-target=/]",
	Short:     "see recent changes on a filer",
	Long: `See recent changes on a filer.

  `,
}

var (
	watchFiler   = cmdWatch.Flag.String("filer", "localhost:8888", "filer hostname:port")
	watchTarget  = cmdWatch.Flag.String("pathPrefix", "/", "path to a folder or file, or common prefix for the folders or files on filer")
	watchStart   = cmdWatch.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	watchPattern = cmdWatch.Flag.String("pattern", "", "full path or just filename pattern, ex: \"/home/?opher\", \"*.pdf\", see https://golang.org/pkg/path/filepath/#Match ")
)

func runWatch(cmd *Command, args []string) bool {

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	var filterFunc func(dir, fname string) bool
	if *watchPattern != "" {
		if strings.Contains(*watchPattern, "/") {
			println("watch path pattern", *watchPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*watchPattern, dir+"/"+fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		} else {
			println("watch file pattern", *watchPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*watchPattern, fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		}
	}

	shouldPrint := func(resp *filer_pb.SubscribeMetadataResponse) bool {
		if filterFunc == nil {
			return true
		}
		if resp.EventNotification.OldEntry == nil && resp.EventNotification.NewEntry == nil {
			return false
		}
		if resp.EventNotification.OldEntry != nil && filterFunc(resp.Directory, resp.EventNotification.OldEntry.Name) {
			return true
		}
		if resp.EventNotification.NewEntry != nil && filterFunc(resp.EventNotification.NewParentPath, resp.EventNotification.NewEntry.Name) {
			return true
		}
		return false
	}

	watchErr := pb.WithFilerClient(*watchFiler, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		stream, err := client.SubscribeMetadata(context.Background(), &filer_pb.SubscribeMetadataRequest{
			ClientName: "watch",
			PathPrefix: *watchTarget,
			SinceNs:    time.Now().Add(-*watchStart).UnixNano(),
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
			if !shouldPrint(resp) {
				continue
			}
			fmt.Printf("%+v\n", resp.EventNotification)
		}

	})
	if watchErr != nil {
		fmt.Printf("watch %s: %v\n", *watchFiler, watchErr)
	}

	return true
}
