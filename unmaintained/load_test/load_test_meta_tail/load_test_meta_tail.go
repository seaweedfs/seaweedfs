package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"io"
	"strconv"
)

var (
	dir       = flag.String("dir", "/tmp", "directory to create files")
	n         = flag.Int("n", 100, "the number of metadata")
	tailFiler = flag.String("filer", "localhost:8888", "the filer address")
	isWrite = flag.Bool("write", false, "only write")
)

func main() {

	flag.Parse()

	if *isWrite {
		startGenerateMetadata()
		return
	}

	expected := 0
	startSubscribeMetadata(func(event *filer_pb.SubscribeMetadataResponse) error {
		if event.Directory != *dir {
			return nil
		}
		name := event.EventNotification.NewEntry.Name
		fmt.Printf("=> %s\n", name)
		id := name[4:]
		if x, err := strconv.Atoi(id); err == nil {
			if x != expected {
				return fmt.Errorf("Expected file%d Actual %s\n", expected, name)
			}
			expected++
		} else {
			return err
		}
		return nil
	})

}

func startGenerateMetadata() {
	pb.WithFilerClient(*tailFiler, grpc.WithInsecure(), func(client filer_pb.SeaweedFilerClient) error {

		for i := 0; i < *n; i++ {
			name := fmt.Sprintf("file%d", i)
			if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
				Directory: *dir,
				Entry: &filer_pb.Entry{
					Name: name,
				},
			}); err != nil {
				fmt.Printf("create entry %s: %v\n", name, err)
				return err
			}
		}

		return nil

	})
}

func startSubscribeMetadata(eachEntryFunc func(event *filer_pb.SubscribeMetadataResponse) error) {

	lastTsNs := int64(0)

	tailErr := pb.WithFilerClient(*tailFiler, grpc.WithInsecure(), func(client filer_pb.SeaweedFilerClient) error {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName: "tail",
			PathPrefix: *dir,
			SinceNs:    lastTsNs,
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
			if err = eachEntryFunc(resp); err != nil {
				return err
			}
			lastTsNs = resp.TsNs
		}

	})
	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}
}
