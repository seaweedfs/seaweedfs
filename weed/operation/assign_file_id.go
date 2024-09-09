package operation

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"sync"
)

type VolumeAssignRequest struct {
	Count               uint64
	Replication         string
	Collection          string
	Ttl                 string
	DiskType            string
	DataCenter          string
	Rack                string
	DataNode            string
	WritableVolumeCount uint32
}

type AssignResult struct {
	Fid       string              `json:"fid,omitempty"`
	Url       string              `json:"url,omitempty"`
	PublicUrl string              `json:"publicUrl,omitempty"`
	GrpcPort  int                 `json:"grpcPort,omitempty"`
	Count     uint64              `json:"count,omitempty"`
	Error     string              `json:"error,omitempty"`
	Auth      security.EncodedJwt `json:"auth,omitempty"`
	Replicas  []Location          `json:"replicas,omitempty"`
}

// This is a proxy to the master server, only for assigning volume ids.
// It runs via grpc to the master server in streaming mode.
// The connection to the master would only be re-established when the last connection has error.
type AssignProxy struct {
	grpcConnection *grpc.ClientConn
	pool           chan *singleThreadAssignProxy
}

func NewAssignProxy(masterFn GetMasterFn, grpcDialOption grpc.DialOption, concurrency int) (ap *AssignProxy, err error) {
	ap = &AssignProxy{
		pool: make(chan *singleThreadAssignProxy, concurrency),
	}
	ap.grpcConnection, err = pb.GrpcDial(context.Background(), masterFn(context.Background()).ToGrpcAddress(), true, grpcDialOption)
	if err != nil {
		return nil, fmt.Errorf("fail to dial %s: %v", masterFn(context.Background()).ToGrpcAddress(), err)
	}
	for i := 0; i < concurrency; i++ {
		ap.pool <- &singleThreadAssignProxy{}
	}
	return ap, nil
}

func (ap *AssignProxy) Assign(primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (ret *AssignResult, err error) {
	p := <-ap.pool
	defer func() {
		ap.pool <- p
	}()

	return p.doAssign(ap.grpcConnection, primaryRequest, alternativeRequests...)
}

type singleThreadAssignProxy struct {
	assignClient master_pb.Seaweed_StreamAssignClient
	sync.Mutex
}

func (ap *singleThreadAssignProxy) doAssign(grpcConnection *grpc.ClientConn, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (ret *AssignResult, err error) {
	ap.Lock()
	defer ap.Unlock()

	if ap.assignClient == nil {
		client := master_pb.NewSeaweedClient(grpcConnection)
		ap.assignClient, err = client.StreamAssign(context.Background())
		if err != nil {
			ap.assignClient = nil
			return nil, fmt.Errorf("fail to create stream assign client: %v", err)
		}
	}

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)
	ret = &AssignResult{}

	for _, request := range requests {
		if request == nil {
			continue
		}
		req := &master_pb.AssignRequest{
			Count:               request.Count,
			Replication:         request.Replication,
			Collection:          request.Collection,
			Ttl:                 request.Ttl,
			DiskType:            request.DiskType,
			DataCenter:          request.DataCenter,
			Rack:                request.Rack,
			DataNode:            request.DataNode,
			WritableVolumeCount: request.WritableVolumeCount,
		}
		if err = ap.assignClient.Send(req); err != nil {
			return nil, fmt.Errorf("StreamAssignSend: %v", err)
		}
		resp, grpcErr := ap.assignClient.Recv()
		if grpcErr != nil {
			return nil, grpcErr
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("StreamAssignRecv: %v", resp.Error)
		}

		ret.Count = resp.Count
		ret.Fid = resp.Fid
		ret.Url = resp.Location.Url
		ret.PublicUrl = resp.Location.PublicUrl
		ret.GrpcPort = int(resp.Location.GrpcPort)
		ret.Error = resp.Error
		ret.Auth = security.EncodedJwt(resp.Auth)
		for _, r := range resp.Replicas {
			ret.Replicas = append(ret.Replicas, Location{
				Url:        r.Url,
				PublicUrl:  r.PublicUrl,
				DataCenter: r.DataCenter,
			})
		}

		if ret.Count <= 0 {
			continue
		}
		break
	}

	return
}

func Assign(masterFn GetMasterFn, grpcDialOption grpc.DialOption, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	for i, request := range requests {
		if request == nil {
			continue
		}

		lastError = WithMasterServerClient(false, masterFn(context.Background()), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
			req := &master_pb.AssignRequest{
				Count:               request.Count,
				Replication:         request.Replication,
				Collection:          request.Collection,
				Ttl:                 request.Ttl,
				DiskType:            request.DiskType,
				DataCenter:          request.DataCenter,
				Rack:                request.Rack,
				DataNode:            request.DataNode,
				WritableVolumeCount: request.WritableVolumeCount,
			}
			resp, grpcErr := masterClient.Assign(context.Background(), req)
			if grpcErr != nil {
				return grpcErr
			}

			if resp.Error != "" {
				return fmt.Errorf("assignRequest: %v", resp.Error)
			}

			ret.Count = resp.Count
			ret.Fid = resp.Fid
			ret.Url = resp.Location.Url
			ret.PublicUrl = resp.Location.PublicUrl
			ret.GrpcPort = int(resp.Location.GrpcPort)
			ret.Error = resp.Error
			ret.Auth = security.EncodedJwt(resp.Auth)
			for _, r := range resp.Replicas {
				ret.Replicas = append(ret.Replicas, Location{
					Url:        r.Url,
					PublicUrl:  r.PublicUrl,
					DataCenter: r.DataCenter,
				})
			}

			return nil

		})

		if lastError != nil {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorChunkAssign).Inc()
			continue
		}

		if ret.Count <= 0 {
			lastError = fmt.Errorf("assign failure %d: %v", i+1, ret.Error)
			continue
		}

		break
	}

	return ret, lastError
}

func LookupJwt(master pb.ServerAddress, grpcDialOption grpc.DialOption, fileId string) (token security.EncodedJwt) {

	WithMasterServerClient(false, master, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		resp, grpcErr := masterClient.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{fileId},
		})
		if grpcErr != nil {
			return grpcErr
		}

		if len(resp.VolumeIdLocations) == 0 {
			return nil
		}

		token = security.EncodedJwt(resp.VolumeIdLocations[0].Auth)

		return nil

	})

	return
}

type StorageOption struct {
	Replication       string
	DiskType          string
	Collection        string
	DataCenter        string
	Rack              string
	DataNode          string
	TtlSeconds        int32
	VolumeGrowthCount uint32
	MaxFileNameLength uint32
	Fsync             bool
	SaveInside        bool
}

func (so *StorageOption) TtlString() string {
	return needle.SecondsToTTL(so.TtlSeconds)
}

func (so *StorageOption) ToAssignRequests(count int) (ar *VolumeAssignRequest, altRequest *VolumeAssignRequest) {
	ar = &VolumeAssignRequest{
		Count:               uint64(count),
		Replication:         so.Replication,
		Collection:          so.Collection,
		Ttl:                 so.TtlString(),
		DiskType:            so.DiskType,
		DataCenter:          so.DataCenter,
		Rack:                so.Rack,
		DataNode:            so.DataNode,
		WritableVolumeCount: so.VolumeGrowthCount,
	}
	if so.DataCenter != "" || so.Rack != "" || so.DataNode != "" {
		altRequest = &VolumeAssignRequest{
			Count:               uint64(count),
			Replication:         so.Replication,
			Collection:          so.Collection,
			Ttl:                 so.TtlString(),
			DiskType:            so.DiskType,
			DataCenter:          "",
			Rack:                "",
			DataNode:            "",
			WritableVolumeCount: so.VolumeGrowthCount,
		}
	}
	return
}
