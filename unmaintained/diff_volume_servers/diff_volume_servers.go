package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	serversStr       = flag.String("volumeServers", "", "comma-delimited list of volume servers to diff the volume against")
	volumeId         = flag.Int("volumeId", -1, "a volume id to diff from servers")
	volumeCollection = flag.String("collection", "", "the volume collection name")
	grpcDialOption   grpc.DialOption
)

/*
	Diff the volume's files across multiple volume servers.
	diff_volume_servers -volumeServers 127.0.0.1:8080,127.0.0.1:8081 -volumeId 5

	Example Output:
	reference 127.0.0.1:8081
	fileId volumeServer message
	5,01617c3f61 127.0.0.1:8080 wrongSize
*/
func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()
	
	util.LoadSecurityConfiguration()
	grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	vid := uint32(*volumeId)
	servers := pb.ServerAddresses(*serversStr).ToAddresses()
	if len(servers) < 2 {
		glog.Fatalf("You must specify more than 1 server\n")
	}
	var referenceServer pb.ServerAddress
	var maxOffset int64
	allFiles := map[pb.ServerAddress]map[types.NeedleId]needleState{}
	for _, addr := range servers {
		files, offset, err := getVolumeFiles(vid, addr)
		if err != nil {
			glog.Fatalf("Failed to copy idx from volume server %s\n", err)
		}
		allFiles[addr] = files
		if offset > maxOffset {
			referenceServer = addr
		}
	}

	same := true
	fmt.Println("reference", referenceServer)
	fmt.Println("fileId volumeServer message")
	for nid, n := range allFiles[referenceServer] {
		for addr, files := range allFiles {
			if addr == referenceServer {
				continue
			}
			var diffMsg string
			n2, ok := files[nid]
			if !ok {
				if n.state == stateDeleted {
					continue
				}
				diffMsg = "missing"
			} else if n2.state != n.state {
				switch n.state {
				case stateDeleted:
					diffMsg = "notDeleted"
				case statePresent:
					diffMsg = "deleted"
				}
			} else if n2.size != n.size {
				diffMsg = "wrongSize"
			} else {
				continue
			}
			same = false

			// fetch the needle details
			var id string
			var err error
			if n.state == statePresent {
				id, err = getNeedleFileId(vid, nid, referenceServer)
			} else {
				id, err = getNeedleFileId(vid, nid, addr)
			}
			if err != nil {
				glog.Fatalf("Failed to get needle info %d from volume server %s\n", nid, err)
			}
			fmt.Println(id, addr, diffMsg)
		}
	}
	if !same {
		os.Exit(1)
	}
}

const (
	stateDeleted uint8 = 1
	statePresent uint8 = 2
)

type needleState struct {
	state uint8
	size  types.Size
}

func getVolumeFiles(v uint32, addr pb.ServerAddress) (map[types.NeedleId]needleState, int64, error) {
	var idxFile *bytes.Reader
	err := operation.WithVolumeServerClient(false, addr, grpcDialOption, func(vs volume_server_pb.VolumeServerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		copyFileClient, err := vs.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
			VolumeId:           v,
			Ext:                ".idx",
			CompactionRevision: math.MaxUint32,
			StopOffset:         math.MaxInt64,
			Collection:         *volumeCollection,
		})
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		for {
			resp, err := copyFileClient.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			buf.Write(resp.FileContent)
		}
		idxFile = bytes.NewReader(buf.Bytes())
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	var maxOffset int64
	files := map[types.NeedleId]needleState{}
	err = idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if offset.IsZero() || size.IsDeleted() {
			files[key] = needleState{
				state: stateDeleted,
				size:  size,
			}
		} else {
			files[key] = needleState{
				state: statePresent,
				size:  size,
			}
		}
		if actual := offset.ToActualOffset(); actual > maxOffset {
			maxOffset = actual
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return files, maxOffset, nil
}

func getNeedleFileId(v uint32, nid types.NeedleId, addr pb.ServerAddress) (string, error) {
	var id string
	err := operation.WithVolumeServerClient(false, addr, grpcDialOption, func(vs volume_server_pb.VolumeServerClient) error {
		resp, err := vs.VolumeNeedleStatus(context.Background(), &volume_server_pb.VolumeNeedleStatusRequest{
			VolumeId: v,
			NeedleId: uint64(nid),
		})
		if err != nil {
			return err
		}
		id = needle.NewFileId(needle.VolumeId(v), resp.NeedleId, resp.Cookie).String()
		return nil
	})
	return id, err
}
