package dash

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

func TestDeleteS3TablesNamespaceForce(t *testing.T) {
	ctx := context.Background()
	client := s3tables.NewManagerClient(s3tablestest.Start(t).Client)
	manager := newS3TablesManager()
	exec := func(op string, req, resp interface{}) {
		t.Helper()
		if err := manager.Execute(ctx, client, op, req, resp, s3_constants.AccountAdminId); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	var bucket s3tables.CreateTableBucketResponse
	exec("CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: "analytics"}, &bucket)
	namespace := []string{"sales"}
	exec("CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: bucket.ARN, Namespace: namespace}, nil)
	// More tables than one ListTables page, so the force path must page.
	const tables = 105
	for i := 0; i < tables; i++ {
		exec("CreateTable", &s3tables.CreateTableRequest{TableBucketARN: bucket.ARN, Namespace: namespace, Name: fmt.Sprintf("t%03d", i), Format: "ICEBERG"}, nil)
	}

	deleted, err := deleteS3TablesNamespace(ctx, manager, client, bucket.ARN, namespace, false)
	var s3Err *s3tables.S3TablesError
	if !errors.As(err, &s3Err) || s3Err.Type != s3tables.ErrCodeNamespaceNotEmpty || deleted != 0 {
		t.Fatalf("delete without force: deleted %d, err %v; want NamespaceNotEmpty", deleted, err)
	}

	deleted, err = deleteS3TablesNamespace(ctx, manager, client, bucket.ARN, namespace, true)
	if err != nil || deleted != tables {
		t.Fatalf("force delete: deleted %d, err %v; want %d tables", deleted, err, tables)
	}
	var namespaces s3tables.ListNamespacesResponse
	exec("ListNamespaces", &s3tables.ListNamespacesRequest{TableBucketARN: bucket.ARN}, &namespaces)
	if len(namespaces.Namespaces) != 0 {
		t.Fatalf("namespaces left after force delete: %+v", namespaces.Namespaces)
	}
}
