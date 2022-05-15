package s3api

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/s3_pb"
)

func (s3a *S3ApiServer) Configure(ctx context.Context, request *s3_pb.ConfigureRequest) (*s3_pb.ConfigureResponse, error) {

	if err := s3a.iam.LoadS3ApiConfigurationFromBytes(request.ConfigurationFileContent); err != nil {
		return nil, err
	}

	return &s3_pb.ConfigureResponse{}, nil

}
