package pb

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// fakeMaster answers a listing either way, so the same assertions can be made
// of a master that streams and one too old to.
type fakeMaster struct {
	master_pb.UnimplementedSeaweedServer
	response *master_pb.VolumeListResponse
	streams  bool
	batch    int
	// extraBatch is sent after the rest, standing for a disk that registered
	// once the topology had gone out.
	extraBatch *master_pb.VolumeListStreamResponse
	// failAfterHeader stands for a master that plainly has the method but
	// gives up mid-stream, reporting the one code that means "ask the old way".
	failAfterHeader bool
}

func (m *fakeMaster) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	return cloneListing(m.response), nil
}

func (m *fakeMaster) VolumeListStream(req *master_pb.VolumeListRequest, stream master_pb.Seaweed_VolumeListStreamServer) error {
	if !m.streams {
		return status_Unimplemented()
	}
	full := cloneListing(m.response)
	header := &master_pb.VolumeListResponse{
		TopologyInfo:      &master_pb.TopologyInfo{Id: full.TopologyInfo.Id},
		VolumeSizeLimitMb: full.VolumeSizeLimitMb,
	}
	// The header names every disk but lists nothing on it.
	for _, dc := range full.TopologyInfo.DataCenterInfos {
		headerDc := &master_pb.DataCenterInfo{Id: dc.Id}
		for _, rack := range dc.RackInfos {
			headerRack := &master_pb.RackInfo{Id: rack.Id}
			for _, node := range rack.DataNodeInfos {
				headerNode := &master_pb.DataNodeInfo{Id: node.Id, DiskInfos: map[string]*master_pb.DiskInfo{}}
				for diskType, disk := range node.DiskInfos {
					headerNode.DiskInfos[diskType] = &master_pb.DiskInfo{Type: diskType, DiskId: disk.DiskId}
				}
				headerRack.DataNodeInfos = append(headerRack.DataNodeInfos, headerNode)
			}
			headerDc.RackInfos = append(headerDc.RackInfos, headerRack)
		}
		header.TopologyInfo.DataCenterInfos = append(header.TopologyInfo.DataCenterInfos, headerDc)
	}
	if err := stream.Send(&master_pb.VolumeListStreamResponse{Header: header}); err != nil {
		return err
	}
	if m.failAfterHeader {
		return status_Unimplemented()
	}

	for _, dc := range full.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType, disk := range node.DiskInfos {
					for start := 0; start < len(disk.VolumeInfos); start += m.batch {
						end := min(start+m.batch, len(disk.VolumeInfos))
						err := stream.Send(&master_pb.VolumeListStreamResponse{
							DataCenter: dc.Id, Rack: rack.Id, DataNode: node.Id, DiskType: diskType,
							VolumeInfos: disk.VolumeInfos[start:end],
						})
						if err != nil {
							return err
						}
					}
					if len(disk.EcShardInfos) > 0 {
						err := stream.Send(&master_pb.VolumeListStreamResponse{
							DataCenter: dc.Id, Rack: rack.Id, DataNode: node.Id, DiskType: diskType,
							EcShardInfos: disk.EcShardInfos,
						})
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	if m.extraBatch != nil {
		if err := stream.Send(m.extraBatch); err != nil {
			return err
		}
	}
	return nil
}

func testListing(volumes int) *master_pb.VolumeListResponse {
	disk := &master_pb.DiskInfo{Type: "", DiskId: 2}
	for i := 1; i <= volumes; i++ {
		disk.VolumeInfos = append(disk.VolumeInfos, &master_pb.VolumeInformationMessage{
			Id: uint32(i), Size: uint64(i) * 100, Collection: "c",
		})
	}
	disk.EcShardInfos = append(disk.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
		Id: 900, Collection: "c", EcIndexBits: 0x3fff,
	})
	return &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: 30000,
		TopologyInfo: &master_pb.TopologyInfo{
			Id: "topo",
			DataCenterInfos: []*master_pb.DataCenterInfo{{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{{
					Id: "rack1",
					DataNodeInfos: []*master_pb.DataNodeInfo{{
						Id:        "10.0.0.1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{"": disk},
					}},
				}},
			}},
		},
	}
}

func dial(t *testing.T, master *fakeMaster) master_pb.SeaweedClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	master_pb.RegisterSeaweedServer(server, master)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return master_pb.NewSeaweedClient(conn)
}

// Whichever way the master answers, a caller sees the same volumes, and sees
// none of them on the topology it is handed.
func TestReceiveVolumeListIsTheSameEitherWay(t *testing.T) {
	const volumes = 250
	for _, streams := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming=%v", streams), func(t *testing.T) {
			client := dial(t, &fakeMaster{response: testListing(volumes), streams: streams, batch: 32})

			var got []uint32
			var ec []uint32
			var topology *master_pb.VolumeListResponse
			err := ReceiveVolumeList(context.Background(), client, &master_pb.VolumeListRequest{},
				func(header *master_pb.VolumeListResponse) error {
					topology = header
					return nil
				},
				func(batch *master_pb.VolumeListStreamResponse) error {
					for _, v := range batch.VolumeInfos {
						got = append(got, v.Id)
					}
					for _, s := range batch.EcShardInfos {
						ec = append(ec, s.Id)
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != volumes {
				t.Errorf("received %d volumes, want %d", len(got), volumes)
			}
			if len(ec) != 1 {
				t.Errorf("received %d ec shards, want 1", len(ec))
			}
			if topology == nil {
				t.Fatal("never told the topology")
			}
			if topology.VolumeSizeLimitMb != 30000 {
				t.Errorf("volume size limit %d, want 30000", topology.VolumeSizeLimitMb)
			}
			disk := topology.TopologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[""]
			if len(disk.VolumeInfos) != 0 || len(disk.EcShardInfos) != 0 {
				t.Errorf("the topology handed over listed %d volumes and %d ec shards, want none",
					len(disk.VolumeInfos), len(disk.EcShardInfos))
			}
		})
	}
}

// Reassembly must put back exactly what an unstreamed listing holds -- in
// particular it must not double the volumes when the master did not stream.
func TestCollectVolumeListRebuildsTheListing(t *testing.T) {
	const volumes = 250
	for _, streams := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming=%v", streams), func(t *testing.T) {
			client := dial(t, &fakeMaster{response: testListing(volumes), streams: streams, batch: 32})

			response, err := CollectVolumeList(context.Background(), client, &master_pb.VolumeListRequest{})
			if err != nil {
				t.Fatal(err)
			}
			disk := response.TopologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[""]
			if len(disk.VolumeInfos) != volumes {
				t.Fatalf("rebuilt %d volumes, want %d", len(disk.VolumeInfos), volumes)
			}
			if len(disk.EcShardInfos) != 1 {
				t.Fatalf("rebuilt %d ec shards, want 1", len(disk.EcShardInfos))
			}
			if disk.DiskId != 2 {
				t.Errorf("rebuilt disk id %d, want 2", disk.DiskId)
			}
			seen := make(map[uint32]int, volumes)
			for _, v := range disk.VolumeInfos {
				seen[v.Id]++
			}
			for id, n := range seen {
				if n != 1 {
					t.Fatalf("volume %d rebuilt %d times", id, n)
				}
			}
		})
	}
}

func status_Unimplemented() error {
	return status.Error(codes.Unimplemented, "this master does not stream volume listings")
}

func cloneListing(r *master_pb.VolumeListResponse) *master_pb.VolumeListResponse {
	return proto.Clone(r).(*master_pb.VolumeListResponse)
}

// A heartbeat can register a disk between the topology going out and the
// batches following it. Those volumes have nowhere to go in the listing being
// rebuilt, but they must not fail it: the scan that reads it runs every 30
// minutes and would lose the whole cluster over one new disk.
func TestCollectVolumeListSurvivesADiskAddedMidStream(t *testing.T) {
	const volumes = 100
	master := &fakeMaster{response: testListing(volumes), streams: true, batch: 32}
	master.extraBatch = &master_pb.VolumeListStreamResponse{
		DataCenter: "dc1", Rack: "rack1", DataNode: "10.0.0.1:8080", DiskType: "ssd",
		VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 5000, Collection: "c"}},
	}
	client := dial(t, master)

	response, err := CollectVolumeList(context.Background(), client, &master_pb.VolumeListRequest{})
	if err != nil {
		t.Fatalf("a disk arriving mid-stream failed the listing: %v", err)
	}
	node := response.TopologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0]
	if len(node.DiskInfos[""].VolumeInfos) != volumes {
		t.Errorf("rebuilt %d volumes, want %d", len(node.DiskInfos[""].VolumeInfos), volumes)
	}
	if _, appeared := node.DiskInfos["ssd"]; appeared {
		t.Error("the listing grew a disk its topology never named")
	}
}

// A stream that has already spoken cannot be started over as an unstreamed
// listing: the caller would be handed the same volumes twice.
func TestReceiveVolumeListDoesNotRestartAStreamThatBegan(t *testing.T) {
	client := dial(t, &fakeMaster{response: testListing(100), streams: true, batch: 32, failAfterHeader: true})

	topologies, batches := 0, 0
	err := ReceiveVolumeList(context.Background(), client, &master_pb.VolumeListRequest{},
		func(*master_pb.VolumeListResponse) error { topologies++; return nil },
		func(*master_pb.VolumeListStreamResponse) error { batches++; return nil })
	if err == nil {
		t.Fatal("a stream that failed after its topology was quietly restarted")
	}
	if topologies != 1 {
		t.Errorf("handed the topology %d times, want 1", topologies)
	}
	if batches != 0 {
		t.Errorf("handed %d volume batches, want none", batches)
	}
}

// A caller's own error must reach it, even when it happens to carry the code
// that means an older master.
func TestReceiveVolumeListDoesNotRestartOnACallersError(t *testing.T) {
	client := dial(t, &fakeMaster{response: testListing(100), streams: true, batch: 32})

	batches := 0
	err := ReceiveVolumeList(context.Background(), client, &master_pb.VolumeListRequest{},
		nil,
		func(*master_pb.VolumeListStreamResponse) error {
			batches++
			return status.Error(codes.Unimplemented, "the caller cannot handle this")
		})
	if err == nil {
		t.Fatal("the caller's error was swallowed and the listing restarted")
	}
	if batches != 1 {
		t.Errorf("called back %d times, want 1 before giving up", batches)
	}
}
