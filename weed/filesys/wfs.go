package filesys

import (
	"bazil.org/fuse/fs"
	"fmt"
	"google.golang.org/grpc"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type WFS struct {
	filer string
}

func NewSeaweedFileSystem(filer string) *WFS {
	return &WFS{
		filer: filer,
	}
}

func (wfs *WFS) Root() (fs.Node, error) {
	return &Dir{Path: "/", wfs: wfs}, nil
}

func (wfs *WFS) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := grpc.Dial(wfs.filer, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", wfs.filer, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}
