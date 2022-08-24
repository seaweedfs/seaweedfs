package s3api

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
)

func (s3a *S3ApiServer) Configure(ctx context.Context, request *s3_pb.S3ConfigureRequest) (*s3_pb.S3ConfigureResponse, error) {

	if err := s3a.iam.LoadS3ApiConfigurationFromBytes(request.S3ConfigurationFileContent); err != nil {
		return nil, err
	}

	return &s3_pb.S3ConfigureResponse{}, nil

}
