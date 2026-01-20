package dash

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// getBucketsPath retrieves the buckets path from filer configuration
// Returns "/buckets" as default if configuration cannot be retrieved
func (s *AdminServer) getBucketsPath() string {
	bucketsPath := "/buckets" // default
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		configResp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		if configResp.DirBuckets != "" {
			bucketsPath = configResp.DirBuckets
		}
		return nil
	})
	if err != nil {
		glog.V(1).Infof("Using default buckets path /buckets (failed to get filer config: %v)", err)
	}
	return bucketsPath
}
