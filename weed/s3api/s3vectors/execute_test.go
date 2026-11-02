package s3vectors

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

func TestExecuteRunsOperationsAsTheIdentity(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), nil, nil)
	s.SetRegion(s3tables.DefaultRegion)
	s.SetAccountID(s3tables.DefaultAccountID)
	ctx := context.Background()
	admin := s3_constants.AccountAdminId

	var created CreateVectorBucketResponse
	if err := s.Execute(ctx, "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "media"}, &created, admin); err != nil {
		t.Fatalf("create: %v", err)
	}
	if want := "arn:aws:s3vectors:us-east-1:000000000000:bucket/media"; created.VectorBucketArn != want {
		t.Fatalf("arn %q, want %q", created.VectorBucketArn, want)
	}
	var list ListVectorBucketsResponse
	if err := s.Execute(ctx, "ListVectorBuckets", &ListVectorBucketsRequest{}, &list, admin); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.VectorBuckets) != 1 || list.VectorBuckets[0].VectorBucketName != "media" {
		t.Fatalf("list = %+v", list.VectorBuckets)
	}

	var e *Error
	err := s.Execute(ctx, "GetVectorBucketPolicy", &GetVectorBucketPolicyRequest{VectorBucketName: "media"}, nil, admin)
	if !errors.As(err, &e) || e.Code != "NotFoundException" {
		t.Fatalf("policy of a bucket without one: %v, want NotFoundException", err)
	}
	if err := s.Execute(ctx, "DeleteVectorBucket", &DeleteVectorBucketRequest{VectorBucketName: "media"}, nil, admin); err != nil {
		t.Fatalf("delete: %v", err)
	}
	err = s.Execute(ctx, "GetVectorBucket", &GetVectorBucketRequest{VectorBucketName: "media"}, nil, admin)
	if !errors.As(err, &e) || e.Code != "NotFoundException" {
		t.Fatalf("get after delete: %v, want NotFoundException", err)
	}
	err = s.Execute(ctx, "TagResource", struct{}{}, nil, admin)
	if !errors.As(err, &e) || e.Code != "ValidationException" {
		t.Fatalf("unknown operation: %v, want ValidationException", err)
	}
}

func TestExecuteChecksTheIdentitysGrants(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), nil, nil)
	ctx := context.Background()
	if err := s.Execute(ctx, "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "media"}, nil, s3_constants.AccountAdminId); err != nil {
		t.Fatalf("create: %v", err)
	}
	var e *Error
	err := s.Execute(ctx, "DeleteVectorBucket", &DeleteVectorBucketRequest{VectorBucketName: "media"}, nil, "someone-else")
	if !errors.As(err, &e) || e.Code != "AccessDeniedException" {
		t.Fatalf("delete by a stranger: %v, want AccessDeniedException", err)
	}
}
