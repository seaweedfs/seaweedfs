//go:build !elastic
// +build !elastic

package command

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func sendToElasticSearchFunc(servers string, esIndex string) (func(resp *filer_pb.SubscribeMetadataResponse) error, error) {
	return func(resp *filer_pb.SubscribeMetadataResponse) error {
		return nil
	}, nil
}
