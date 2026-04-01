package shell

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
)

const s3TablesDefaultRegion = ""
const timeFormat = "2006-01-02T15:04:05Z07:00"

func withFilerClient(commandEnv *CommandEnv, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(conn)
		return fn(client)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}

func executeS3Tables(commandEnv *CommandEnv, operation string, req interface{}, resp interface{}, accountID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return withFilerClient(commandEnv, func(client filer_pb.SeaweedFilerClient) error {
		manager := s3tables.NewManager()
		mgrClient := s3tables.NewManagerClient(client)
		return manager.Execute(ctx, mgrClient, operation, req, resp, accountID)
	})
}

func parseS3TablesError(err error) error {
	if err == nil {
		return nil
	}
	var s3Err *s3tables.S3TablesError
	if errors.As(err, &s3Err) {
		if s3Err.Message != "" {
			return fmt.Errorf("%s: %s", s3Err.Type, s3Err.Message)
		}
		return fmt.Errorf("%s", s3Err.Type)
	}
	return err
}

func parseS3TablesTags(value string) (map[string]string, error) {
	parsed := make(map[string]string)
	for _, kv := range strings.Split(value, ",") {
		if kv == "" {
			continue
		}
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid tag: %s", kv)
		}
		parsed[parts[0]] = parts[1]
	}
	if err := s3tables.ValidateTags(parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func parseS3TablesTagKeys(value string) ([]string, error) {
	var keys []string
	for _, key := range strings.Split(value, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("tagKeys are required")
	}
	return keys, nil
}

func buildS3TablesBucketARN(bucketName, accountID string) (string, error) {
	return s3tables.BuildBucketARN(s3TablesDefaultRegion, accountID, bucketName)
}

func buildS3TablesTableARN(bucketName, namespace, tableName, accountID string) (string, error) {
	return s3tables.BuildTableARN(s3TablesDefaultRegion, accountID, bucketName, namespace, tableName)
}
