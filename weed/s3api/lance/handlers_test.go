package lance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// openAuthenticator stands in for a gateway with no IAM configured, which is the
// mode the namespace falls open in.
type openAuthenticator struct{}

func (openAuthenticator) AuthenticateRequest(*http.Request) (string, interface{}, s3err.ErrorCode) {
	return s3tables.DefaultAccountID, nil, s3err.ErrNone
}

func (openAuthenticator) DefaultAllow() bool { return true }

type testHarness struct {
	router *mux.Router
	filer  *s3tablestest.MemFiler
	admin  *s3tables.Manager
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	filer := s3tablestest.Start(t)
	server := NewServer(s3tables.NewManagerClient(filer.Client), openAuthenticator{})
	server.SetS3Endpoint("http://127.0.0.1:8333")
	server.SetS3Region(s3tables.DefaultRegion)
	router := mux.NewRouter().SkipClean(true)
	server.RegisterRoutes(router)

	// Table buckets are created by an operator, not by a namespace client, so
	// the harness seeds them the way the shell and admin console would.
	admin := s3tables.NewManager()
	admin.SetTrusted(true)
	return &testHarness{router: router, filer: filer, admin: admin}
}

// createBucket seeds a table bucket the Lance namespace can then be pointed at.
func (h *testHarness) createBucket(t *testing.T, name string) {
	t.Helper()
	var resp s3tables.CreateTableBucketResponse
	err := h.admin.Execute(t.Context(), s3tables.NewManagerClient(h.filer.Client), "CreateTableBucket",
		&s3tables.CreateTableBucketRequest{Name: name}, &resp, "")
	if err != nil {
		t.Fatalf("create table bucket %s: %v", name, err)
	}
}

func (h *testHarness) bucketARN(t *testing.T, name string) string {
	t.Helper()
	arn, err := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3tables.DefaultAccountID, name)
	if err != nil {
		t.Fatalf("build arn: %v", err)
	}
	return arn
}

func (h *testHarness) do(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, req)
	return recorder
}

func (h *testHarness) mustDo(t *testing.T, method, target, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	recorder := h.do(t, method, target, body)
	if recorder.Code != want {
		t.Fatalf("%s %s = %d (%s), want %d", method, target, recorder.Code, recorder.Body.String(), want)
	}
	return recorder
}

func decode[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return out
}

// The lifecycle a Lance client drives: create the namespace, declare the table
// before any data exists, resolve it to a location, then deregister and bring it
// back by registering the same location.
func TestTableLifecycle(t *testing.T) {
	h := newTestHarness(t)

	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)

	declared := decode[DeclareTableResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/declare", `{}`, http.StatusOK))
	if declared.Location != "s3://analytics/sales/orders" {
		t.Fatalf("declared location = %q", declared.Location)
	}
	// The endpoint is plaintext, so a client that does not get allow_http fails
	// with what looks like a credential error.
	if declared.StorageOptions["aws_endpoint"] != "http://127.0.0.1:8333" ||
		declared.StorageOptions["allow_http"] != "true" {
		t.Fatalf("storage options = %v", declared.StorageOptions)
	}

	described := decode[DescribeTableResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/describe?check_declared=true", `{}`, http.StatusOK))
	if described.Location != declared.Location {
		t.Fatalf("described location = %q, want %q", described.Location, declared.Location)
	}
	if described.IsOnlyDeclared == nil || !*described.IsOnlyDeclared {
		t.Fatalf("is_only_declared = %v, want true for a table with no data", described.IsOnlyDeclared)
	}

	listed := decode[ListTablesResponse](t,
		h.mustDo(t, http.MethodGet, "/v1/namespace/analytics$sales/table/list", "", http.StatusOK))
	if len(listed.Tables) != 1 || listed.Tables[0] != "analytics$sales$orders" {
		t.Fatalf("listed tables = %v, want the full identifier", listed.Tables)
	}

	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/exists", `{}`, http.StatusOK)

	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/deregister", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/exists", `{}`, http.StatusNotFound)
	afterDrop := decode[ListTablesResponse](t,
		h.mustDo(t, http.MethodGet, "/v1/namespace/analytics$sales/table/list", "", http.StatusOK))
	if len(afterDrop.Tables) != 0 {
		t.Fatalf("deregistered table still listed: %v", afterDrop.Tables)
	}
	// Deregistering preserves the data. The catalog entry is the dataset
	// directory, so dropping it would take the dataset with it.
	if h.filer.Get(s3tables.GetNamespacePath("analytics", "sales"), "orders") == nil {
		t.Fatal("deregister deleted the dataset directory")
	}

	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/register",
		`{"location":"s3://analytics/sales/orders"}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/exists", `{}`, http.StatusOK)

	// Dropping is the operation that does remove the data.
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/drop", `{}`, http.StatusOK)
	if h.filer.Get(s3tables.GetNamespacePath("analytics", "sales"), "orders") != nil {
		t.Fatal("drop left the dataset directory behind")
	}
}

// Repointing a registered name at another dataset must not take the dataset it
// used to name with it.
func TestRegisterOverwriteKeepsTheOldDataset(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/declare", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$archive/declare", `{}`, http.StatusOK)

	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/register",
		`{"location":"s3://analytics/sales/archive","mode":"Overwrite"}`, http.StatusOK)

	described := decode[DescribeTableResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/describe", `{}`, http.StatusOK))
	if described.Location != "s3://analytics/sales/archive" {
		t.Fatalf("location after repointing = %q", described.Location)
	}
	if h.filer.Get(s3tables.GetNamespacePath("analytics", "sales"), "orders") == nil {
		t.Fatal("repointing deleted the dataset the name used to hold")
	}
}

func TestNamespaceListing(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$finance/create", `{}`, http.StatusOK)

	roots := decode[ListNamespacesResponse](t,
		h.mustDo(t, http.MethodGet, "/v1/namespace/$/list", "", http.StatusOK))
	if len(roots.Namespaces) != 1 || roots.Namespaces[0] != "analytics" {
		t.Fatalf("root listing = %v, want the table buckets", roots.Namespaces)
	}

	children := decode[ListNamespacesResponse](t,
		h.mustDo(t, http.MethodGet, "/v1/namespace/analytics/list", "", http.StatusOK))
	if len(children.Namespaces) != 2 {
		t.Fatalf("bucket listing = %v, want two namespaces", children.Namespaces)
	}

	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/exists", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$nope/exists", `{}`, http.StatusNotFound)

	// Fail mode reports a missing namespace as 400 on this operation, which is
	// what the spec asks for; Skip reports success.
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$nope/drop", `{}`, http.StatusBadRequest)
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$nope/drop", `{"mode":"Skip"}`, http.StatusNoContent)
}

func TestCreateNamespaceModes(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)

	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusConflict)
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{"mode":"ExistOk"}`, http.StatusOK)
	// snake_case is the other spelling the spec accepts.
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{"mode":"exist_ok"}`, http.StatusOK)

	h.mustDo(t, http.MethodPost, "/v1/namespace/$/create", `{}`, http.StatusBadRequest)
}

// A Lance client must never resolve an Iceberg table's location, or it would
// write a dataset over a table another engine owns.
func TestIcebergTablesAreInvisible(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$vectors/declare", `{}`, http.StatusOK)

	manager := h.admin
	var created s3tables.CreateTableResponse
	err := manager.Execute(t.Context(), s3tables.NewManagerClient(h.filer.Client), "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN:   h.bucketARN(t, "analytics"),
		Namespace:        []string{"sales"},
		Name:             "ledger",
		Format:           s3tables.FormatIceberg,
		MetadataLocation: "s3://analytics/sales/ledger/metadata/v1.metadata.json",
	}, &created, "")
	if err != nil {
		t.Fatalf("create iceberg table: %v", err)
	}

	listed := decode[ListTablesResponse](t,
		h.mustDo(t, http.MethodGet, "/v1/namespace/analytics$sales/table/list", "", http.StatusOK))
	if len(listed.Tables) != 1 || listed.Tables[0] != "analytics$sales$vectors" {
		t.Fatalf("listing = %v, want only the lance table", listed.Tables)
	}

	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$ledger/describe", `{}`, http.StatusNotFound)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$ledger/exists", `{}`, http.StatusNotFound)

	// Declaring over the Iceberg table must not quietly succeed and hand the
	// Lance client a directory another format owns.
	recorder := h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$ledger/declare", `{}`, http.StatusConflict)
	if got := decode[errorResponse](t, recorder); got.Code != codeTableAlreadyExists {
		t.Fatalf("error code = %d, want %d", got.Code, codeTableAlreadyExists)
	}
}

// The route and the body naming different objects is a bad request, not a silent
// preference for one of them.
func TestRouteAndBodyMustAgree(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)

	recorder := h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/declare",
		`{"id":["analytics","sales","other"]}`, http.StatusBadRequest)
	if got := decode[errorResponse](t, recorder); got.Code != codeInvalidInput {
		t.Fatalf("error code = %d, want %d", got.Code, codeInvalidInput)
	}
}

// The data plane needs Lance format support that does not exist in Go, so it
// answers with the spec's own Unsupported code rather than a bare 404.
func TestDataPlaneIsUnsupported(t *testing.T) {
	h := newTestHarness(t)
	recorder := h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/query", `{}`, http.StatusNotImplemented)
	if got := decode[errorResponse](t, recorder); got.Code != codeUnsupported {
		t.Fatalf("error code = %d, want %d", got.Code, codeUnsupported)
	}
}
