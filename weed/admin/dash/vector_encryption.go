package dash

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (s *AdminServer) vectorEncryptionConfigured(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	endpoints := []string{}
	if s.s3PublicEndpoint != "" {
		endpoints = append(endpoints, s.s3PublicEndpoint)
	} else if s.masterClient != nil {
		err := s.masterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
			response, err := client.ListClusterNodes(ctx, s.listClusterNodesRequest(cluster.S3Type))
			if err != nil {
				return err
			}
			for _, node := range response.ClusterNodes {
				endpoints = append(endpoints, "http://"+pb.ServerAddress(node.Address).ToHttpAddress())
			}
			return nil
		})
		if err != nil {
			return false
		}
	}
	if len(endpoints) == 0 {
		return false
	}
	for _, endpoint := range endpoints {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/status?features", nil)
		if err != nil {
			return false
		}
		response, err := util_http.GetGlobalHttpClient().Do(request)
		if err != nil {
			return false
		}
		var features struct {
			SSES3 bool `json:"sseS3"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&features)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || err != nil || !features.SSES3 {
			return false
		}
	}
	return true
}
