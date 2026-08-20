// Integration tests for the Lance Namespace REST server, driven against a live
// gateway rather than an in-memory filer. The bugs this surface has produced -
// a deregister that deleted the dataset, an S3 door that refused every Lance
// file - all looked fine against a fake.
package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// lanceCall posts to the Lance namespace and returns the status and body.
func lanceCall(t *testing.T, env *TestEnvironment, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, env.LanceURL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, payload
}

func lanceMust(t *testing.T, env *TestEnvironment, method, path, body string, want int) []byte {
	t.Helper()
	status, payload := lanceCall(t, env, method, path, body)
	if status != want {
		t.Fatalf("%s %s = %d (%s), want %d", method, path, status, payload, want)
	}
	return payload
}

// filerEntryExists reports whether a path exists on storage, which is how these
// tests tell "the catalog forgot the table" from "the data is gone".
func filerEntryExists(t *testing.T, env *TestEnvironment, path string) bool {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d%s", env.filerPort, path)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("filer GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func lanceTestBucket(t *testing.T, env *TestEnvironment, prefix string) string {
	t.Helper()
	bucket := prefix + "-" + randomSuffix()
	createTableBucket(t, env, bucket, s3tables.FormatLance)
	return bucket
}

func TestLanceNamespaceLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-ns")

	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$sales/create", `{}`, http.StatusOK)
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$sales/exists", `{}`, http.StatusOK)

	// The root lists table buckets, which is the first namespace level here.
	var roots struct {
		Namespaces []string `json:"namespaces"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodGet, "/v1/namespace/$/list", "", http.StatusOK), &roots); err != nil {
		t.Fatalf("decode root listing: %v", err)
	}
	found := false
	for _, name := range roots.Namespaces {
		if name == bucket {
			found = true
		}
	}
	if !found {
		t.Fatalf("root listing %v does not include %s", roots.Namespaces, bucket)
	}

	var children struct {
		Namespaces []string `json:"namespaces"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodGet, "/v1/namespace/"+bucket+"/list", "", http.StatusOK), &children); err != nil {
		t.Fatalf("decode namespace listing: %v", err)
	}
	if len(children.Namespaces) != 1 || children.Namespaces[0] != "sales" {
		t.Fatalf("namespace listing = %v, want [sales]", children.Namespaces)
	}

	// A namespace is never created as a side effect of naming one inside a
	// bucket that does not exist.
	status, _ := lanceCall(t, env, http.MethodPost, "/v1/namespace/nosuchbucket$ns/create", `{}`)
	if status != http.StatusNotFound {
		t.Fatalf("create under a missing bucket = %d, want 404", status)
	}
}

func TestLanceTableLifecyclePreservesData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-tbl")
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$ml/create", `{}`, http.StatusOK)

	table := "/v1/table/" + bucket + "$ml$vectors"
	var declared struct {
		Location       string            `json:"location"`
		StorageOptions map[string]string `json:"storage_options"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodPost, table+"/declare", `{}`, http.StatusOK), &declared); err != nil {
		t.Fatalf("decode declare: %v", err)
	}
	want := fmt.Sprintf("s3://%s/ml/vectors", bucket)
	if declared.Location != want {
		t.Fatalf("declared location = %q, want %q", declared.Location, want)
	}

	datasetPath := fmt.Sprintf("/buckets/%s/ml/vectors/", bucket)
	if !filerEntryExists(t, env, datasetPath) {
		t.Fatal("declare did not create the dataset directory")
	}

	var listed struct {
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodGet, "/v1/namespace/"+bucket+"$ml/table/list", "", http.StatusOK), &listed); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listed.Tables) != 1 || listed.Tables[0] != bucket+"$ml$vectors" {
		t.Fatalf("table listing = %v, want the full identifier", listed.Tables)
	}

	// Deregistering forgets the table but keeps every byte. The catalog entry is
	// the dataset directory, so a drop of the entry would take the data too.
	lanceMust(t, env, http.MethodPost, table+"/deregister", `{}`, http.StatusOK)
	lanceMust(t, env, http.MethodPost, table+"/exists", `{}`, http.StatusNotFound)
	if !filerEntryExists(t, env, datasetPath) {
		t.Fatal("deregister deleted the dataset")
	}

	registerBody := fmt.Sprintf(`{"location":%q}`, want)
	lanceMust(t, env, http.MethodPost, table+"/register", registerBody, http.StatusOK)
	lanceMust(t, env, http.MethodPost, table+"/exists", `{}`, http.StatusOK)

	// Dropping is the operation that does remove the data.
	lanceMust(t, env, http.MethodPost, table+"/drop", `{}`, http.StatusOK)
	if filerEntryExists(t, env, datasetPath) {
		t.Fatal("drop left the dataset behind")
	}
}

// A Lance client must never resolve an Iceberg table's location, or it writes a
// dataset over a table another engine owns.
func TestLanceRefusesIcebergTables(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-mixed")
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$mixed/create", `{}`, http.StatusOK)

	createIcebergTable(t, env, bucket, "mixed", "ledger")

	status, _ := lanceCall(t, env, http.MethodPost, "/v1/table/"+bucket+"$mixed$ledger/describe", `{}`)
	if status != http.StatusNotFound {
		t.Fatalf("describing an iceberg table through lance = %d, want 404", status)
	}
	status, _ = lanceCall(t, env, http.MethodPost, "/v1/table/"+bucket+"$mixed$ledger/declare", `{}`)
	if status != http.StatusConflict {
		t.Fatalf("declaring over an iceberg table = %d, want 409", status)
	}

	var listed struct {
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodGet, "/v1/namespace/"+bucket+"$mixed/table/list", "", http.StatusOK), &listed); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listed.Tables) != 0 {
		t.Fatalf("lance listing shows iceberg tables: %v", listed.Tables)
	}
}

// The S3 door validates every object written into a table bucket. A Lance
// dataset's files have to get through it, or the catalog is decorative.
func TestLanceFilesAreAcceptedByTheS3Door(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-layout")
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$ml/create", `{}`, http.StatusOK)
	lanceMust(t, env, http.MethodPost, "/v1/table/"+bucket+"$ml$vectors/declare", `{}`, http.StatusOK)

	accepted := []string{
		"ml/vectors/data/0111111010111000110101116.lance",
		"ml/vectors/_versions/18446744073709551614.manifest",
		"ml/vectors/_versions/18446744073709551614.manifest-a3a292ad",
		"ml/vectors/_transactions/0-ddb27ab7.txn",
		"ml/vectors/_indices/85814508-fts/index.idx",
		"ml/vectors/.lance-reserved",
	}
	for _, object := range accepted {
		if status := putS3Object(t, env, bucket, object); status != http.StatusOK {
			t.Errorf("PUT %s = %d, want 200", object, status)
		}
	}

	// The guard still rejects files that belong to no table layout.
	for _, object := range []string{"ml/vectors/random.txt", "ml/vectors/notadir/x.lance"} {
		if status := putS3Object(t, env, bucket, object); status == http.StatusOK {
			t.Errorf("PUT %s = 200, want a rejection", object)
		}
	}
}

// A Lance commit is a conditional PUT of the next manifest: object_store turns
// PutMode::Create into If-None-Match: *, and lance treats the refusal as a
// commit conflict and rebases. So the whole safety of concurrent writers rests
// on this store answering that precondition atomically. It does, because the
// gateway reduces the header to a filer WriteCondition evaluated at the
// object's owner under a per-path lock.
//
// This is the reason the namespace does not manage versions itself.
func TestLanceCommitPreconditionAdmitsOneWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-commit")
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$ml/create", `{}`, http.StatusOK)
	lanceMust(t, env, http.MethodPost, "/v1/table/"+bucket+"$ml$vectors/declare", `{}`, http.StatusOK)

	const writers = 8
	var wg sync.WaitGroup
	statuses := make([]int, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i] = putS3ObjectIfAbsent(t, env, bucket, "ml/vectors/_versions/1.manifest")
		}(i)
	}
	wg.Wait()

	won, refused := 0, 0
	for _, status := range statuses {
		switch status {
		case http.StatusOK:
			won++
		case http.StatusPreconditionFailed:
			refused++
		default:
			t.Fatalf("unexpected status %d committing a manifest", status)
		}
	}
	if won != 1 {
		t.Fatalf("%d writers committed version 1; exactly one may win, %d were refused", won, refused)
	}
}

// The namespace answers managed_versioning=false: the dataset owns its version
// history, and a reader that never goes through this catalog still sees all of
// it.
func TestLanceDoesNotManageVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	env := sharedEnv
	bucket := lanceTestBucket(t, env, "lance-ver")
	lanceMust(t, env, http.MethodPost, "/v1/namespace/"+bucket+"$ml/create", `{}`, http.StatusOK)
	table := "/v1/table/" + bucket + "$ml$vectors"

	var declared struct {
		ManagedVersioning bool `json:"managed_versioning"`
	}
	if err := json.Unmarshal(lanceMust(t, env, http.MethodPost, table+"/declare", `{}`, http.StatusOK), &declared); err != nil {
		t.Fatalf("decode declare: %v", err)
	}
	if declared.ManagedVersioning {
		t.Fatal("managed_versioning must be false; the dataset owns its versions")
	}

	// The version ops answer with the spec's Unsupported code rather than a
	// bare 404, so a client that asks learns why.
	status, _ := lanceCall(t, env, http.MethodPost, table+"/version/list", `{}`)
	if status != http.StatusNotImplemented {
		t.Fatalf("version/list = %d, want 501", status)
	}
}

// putS3ObjectIfAbsent writes an object only if the key is free, the way a Lance
// commit does, and returns the status.
func putS3ObjectIfAbsent(t *testing.T, env *TestEnvironment, bucket, object string) int {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/%s/%s", env.s3Port, bucket, object)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte("manifest")))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("If-None-Match", "*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", object, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// putS3Object writes an object through the S3 gateway and returns the status.
func putS3Object(t *testing.T, env *TestEnvironment, bucket, object string) int {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/%s/%s", env.s3Port, bucket, object)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", object, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// createIcebergTable registers an ordinary Iceberg table so the mixed-catalog
// tests have one to be confused by.
func createIcebergTable(t *testing.T, env *TestEnvironment, bucket, namespace, name string) {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}}`, name)
	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables", env.IcebergURL(), bucket, namespace)
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create iceberg table: %v", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create iceberg table = %d: %s", resp.StatusCode, payload)
	}
}
