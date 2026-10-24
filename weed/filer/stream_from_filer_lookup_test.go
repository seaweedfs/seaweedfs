package filer

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// lookupOnlyMaster announces no volumes on KeepConnected, so the client's
// cache stays empty, but answers LookupVolume with the one volume server.
type lookupOnlyMaster struct {
	master_pb.UnimplementedSeaweedServer
	volumeUrl string
}

func (m *lookupOnlyMaster) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (m *lookupOnlyMaster) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	resp := &master_pb.LookupVolumeResponse{}
	for _, id := range req.VolumeOrFileIds {
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: id,
			Locations:      []*master_pb.Location{{Url: m.volumeUrl, PublicUrl: m.volumeUrl}},
		})
	}
	return resp, nil
}

// A filer's cached volume map can lose a volume that still exists: a failed
// read invalidates every cached location of it, and the master only announces
// changes. Readers built from the filer's MasterClient must then ask the master
// instead of failing until the process restarts.
func TestChunkStreamReaderFromFilerAsksMasterOnCacheMiss(t *testing.T) {
	content := []byte("metadata log chunk")
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	t.Cleanup(volume.Close)

	master := &lookupOnlyMaster{volumeUrl: strings.TrimPrefix(volume.URL, "http://")}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, master)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}

	mc := wdclient.NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", cluster.FilerType, pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))}),
	)
	connecting, stopConnecting := context.WithCancel(context.Background())
	t.Cleanup(stopConnecting)
	go mc.KeepConnectedToMaster(connecting)
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWaiting()
	mc.WaitUntilConnected(waiting)
	if waiting.Err() != nil {
		t.Fatal("the master client never connected to the fake master")
	}

	chunks := []*filer_pb.FileChunk{{FileId: "1,0123456789", Offset: 0, Size: uint64(len(content))}}
	got, err := io.ReadAll(NewChunkStreamReaderFromFiler(context.Background(), mc, chunks))
	if err != nil {
		t.Fatalf("read with volume 1 missing from the cache: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("read %q, want %q", got, content)
	}
}
