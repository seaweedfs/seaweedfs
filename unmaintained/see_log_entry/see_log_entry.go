package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	logdataFile = flag.String("logdata", "", "log data file saved under "+filer.SystemLogDir)
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	dst, err := os.OpenFile(*logdataFile, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open %s: %v", *logdataFile, err)
	}
	defer dst.Close()

	err = walkLogEntryFile(dst)
	if err != nil {
		log.Fatalf("failed to visit %s: %v", *logdataFile, err)
	}

}

func walkLogEntryFile(dst *os.File) error {

	sizeBuf := make([]byte, 4)

	for {
		if n, err := dst.Read(sizeBuf); n != 4 {
			if err == io.EOF {
				return nil
			}
			return err
		}

		size := util.BytesToUint32(sizeBuf)

		data := make([]byte, int(size))

		if n, err := dst.Read(data); n != len(data) {
			return err
		}

		logEntry := &filer_pb.LogEntry{}
		err := proto.Unmarshal(data, logEntry)
		if err != nil {
			log.Printf("unexpected unmarshal filer_pb.LogEntry: %v", err)
			return nil
		}

		event := &filer_pb.SubscribeMetadataResponse{}
		err = proto.Unmarshal(logEntry.Data, event)
		if err != nil {
			log.Printf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return nil
		}

		// convert timestamp from nanoseconds to time.Time
		timeStamp := time.Unix(0, int64(logEntry.TsNs))
		timeStr := timeStamp.Format(time.RFC3339)

		switch {
		case event.EventNotification.NewEntry != nil && event.EventNotification.OldEntry == nil:
			fmt.Printf("file create, dir: %s: file: %+s, ts: %d, time: %s\n", event.Directory, event.EventNotification.NewEntry.Name, event.TsNs, timeStr)
		case event.EventNotification.NewEntry != nil && event.EventNotification.OldEntry != nil:
			fmt.Printf("file update: dir: %s, file: %+s, ts: %d, time: %s\n", event.Directory, event.EventNotification.NewEntry.Name, event.TsNs, timeStr)
		case event.EventNotification.NewEntry == nil && event.EventNotification.OldEntry != nil:
			fmt.Printf("file delete: dir: %s, file: %+s, ts: %d, time: %s\n", event.Directory, event.EventNotification.OldEntry.Name, event.TsNs, timeStr)
		case event.EventNotification.NewEntry == nil && event.EventNotification.OldEntry != nil:
		default:
			fmt.Printf("Unknown event: %v\n", event)
		}
	}

}
