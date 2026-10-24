package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// The AWS CLI and SDK address these operations by REST path, not by
// X-Amz-Target, so the paths have to match the published bindings exactly.
func TestMaintenanceRestPathsMatchAWSBindings(t *testing.T) {
	const arnPattern = "arn:aws:s3tables:[^/:]*:[^/:]*:bucket/[^/]+"

	router := mux.NewRouter()
	matched := ""
	for _, route := range []struct {
		method    string
		path      string
		operation string
	}{
		{http.MethodPut, "/buckets/{tableBucketARN:" + arnPattern + "}/maintenance/{type}", "PutTableBucketMaintenanceConfiguration"},
		{http.MethodGet, "/buckets/{tableBucketARN:" + arnPattern + "}/maintenance", "GetTableBucketMaintenanceConfiguration"},
		{http.MethodPut, "/tables/{tableBucketARN:" + arnPattern + "}/{namespace}/{name}/maintenance/{type}", "PutTableMaintenanceConfiguration"},
		{http.MethodGet, "/tables/{tableBucketARN:" + arnPattern + "}/{namespace}/{name}/maintenance", "GetTableMaintenanceConfiguration"},
		{http.MethodGet, "/tables/{tableBucketARN:" + arnPattern + "}/{namespace}/{name}/maintenance-job-status", "GetTableMaintenanceJobStatus"},
	} {
		operation := route.operation
		router.Methods(route.method).Path(route.path).HandlerFunc(func(http.ResponseWriter, *http.Request) {
			matched = operation
		})
	}

	cases := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodPut, "/buckets/" + testTableBucketARN + "/maintenance/icebergUnreferencedFileRemoval", "PutTableBucketMaintenanceConfiguration"},
		{http.MethodGet, "/buckets/" + testTableBucketARN + "/maintenance", "GetTableBucketMaintenanceConfiguration"},
		{http.MethodPut, "/tables/" + testTableBucketARN + "/sales/orders/maintenance/icebergCompaction", "PutTableMaintenanceConfiguration"},
		{http.MethodGet, "/tables/" + testTableBucketARN + "/sales/orders/maintenance", "GetTableMaintenanceConfiguration"},
		{http.MethodGet, "/tables/" + testTableBucketARN + "/sales/orders/maintenance-job-status", "GetTableMaintenanceJobStatus"},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			matched = ""
			router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.path, nil))
			if matched != tc.want {
				t.Errorf("path %s %s routed to %q, want %q", tc.method, tc.path, matched, tc.want)
			}
		})
	}
}

func TestBuildPutTableMaintenanceConfigurationRequest(t *testing.T) {
	body := `{"value":{"status":"enabled","settings":{"icebergCompaction":{"targetFileSizeMB":512}}}}`
	req := httptest.NewRequest(http.MethodPut, "/tables/"+testTableBucketARN+"/sales/orders/maintenance/icebergCompaction", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{
		"tableBucketARN": testTableBucketARN,
		"namespace":      "sales",
		"name":           "orders",
		"type":           "icebergCompaction",
	})

	built, err := buildPutTableMaintenanceConfigurationRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, ok := built.(*s3tables.PutTableMaintenanceConfigurationRequest)
	if !ok {
		t.Fatalf("unexpected request type %T", built)
	}
	if got.TableBucketARN != testTableBucketARN || got.Name != "orders" {
		t.Errorf("path params not applied: %+v", got)
	}
	if len(got.Namespace) != 1 || got.Namespace[0] != "sales" {
		t.Errorf("expected namespace [sales], got %v", got.Namespace)
	}
	if got.Type != s3tables.MaintenanceTypeIcebergCompaction {
		t.Errorf("expected type from the path, got %q", got.Type)
	}
	if got.Value == nil || got.Value.Settings == nil || got.Value.Settings.IcebergCompaction == nil {
		t.Fatalf("expected the body value decoded, got %+v", got.Value)
	}
	if size := got.Value.Settings.IcebergCompaction.TargetFileSizeMB; size == nil || *size != 512 {
		t.Errorf("expected targetFileSizeMB=512, got %v", size)
	}
}

func TestBuildPutTableBucketMaintenanceConfigurationRequest(t *testing.T) {
	body := `{"value":{"status":"disabled"}}`
	req := httptest.NewRequest(http.MethodPut, "/buckets/"+testTableBucketARN+"/maintenance/icebergUnreferencedFileRemoval", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{
		"tableBucketARN": testTableBucketARN,
		"type":           "icebergUnreferencedFileRemoval",
	})

	built, err := buildPutTableBucketMaintenanceConfigurationRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := built.(*s3tables.PutTableBucketMaintenanceConfigurationRequest)
	if got.TableBucketARN != testTableBucketARN {
		t.Errorf("expected the ARN from the path, got %q", got.TableBucketARN)
	}
	if got.Type != s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval {
		t.Errorf("expected the type from the path, got %q", got.Type)
	}
	if got.Value == nil || got.Value.Status != s3tables.MaintenanceStatusDisabled {
		t.Errorf("expected the body value decoded, got %+v", got.Value)
	}
}

func TestBuildMaintenanceRequestsRejectBadNamespace(t *testing.T) {
	for _, build := range map[string]func(*http.Request) (interface{}, error){
		"GetTableMaintenanceConfiguration": buildGetTableMaintenanceConfigurationRequest,
		"GetTableMaintenanceJobStatus":     buildGetTableMaintenanceJobStatusRequest,
	} {
		req := httptest.NewRequest(http.MethodGet, "/tables/"+testTableBucketARN+"/Bad/orders/maintenance", nil)
		req = mux.SetURLVars(req, map[string]string{
			"tableBucketARN": testTableBucketARN,
			"namespace":      "InvalidNamespace",
			"name":           "orders",
		})
		if _, err := build(req); err == nil {
			t.Error("expected an invalid namespace to be rejected")
		}
	}
}
