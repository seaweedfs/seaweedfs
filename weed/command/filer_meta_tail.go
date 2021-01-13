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
	cmdFilerMetaTail.Run = runFilerMetaTail // break init cycle
}

var cmdFilerMetaTail = &Command{
	UsageLine: "filer.meta.tail [-filer=localhost:8888] [-target=/]",
	Short:     "see recent changes on a filer",
	Long: `See recent changes on a filer.

  `,
}

var (
	tailFiler   = cmdFilerMetaTail.Flag.String("filer", "localhost:8888", "filer hostname:port")
	tailTarget  = cmdFilerMetaTail.Flag.String("pathPrefix", "/", "path to a folder or file, or common prefix for the folders or files on filer")
	tailStart   = cmdFilerMetaTail.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	tailPattern = cmdFilerMetaTail.Flag.String("pattern", "", "full path or just filename pattern, ex: \"/home/?opher\", \"*.pdf\", see https://golang.org/pkg/path/filepath/#Match ")
)

func runFilerMetaTail(cmd *Command, args []string) bool {

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	var filterFunc func(dir, fname string) bool
	if *tailPattern != "" {
		if strings.Contains(*tailPattern, "/") {
			println("watch path pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, dir+"/"+fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		} else {
			println("watch file pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, fname)
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

	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		fmt.Printf("dir:%s %+v\n", resp.Directory, resp.EventNotification)
		return nil
	}

	tailErr := pb.WithFilerClient(*tailFiler, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName: "tail",
			PathPrefix: *tailTarget,
			SinceNs:    time.Now().Add(-*tailStart).UnixNano(),
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
			if err = eachEntryFunc(resp); err != nil {
				return err
			}
		}

	})
	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}

	return true
}
