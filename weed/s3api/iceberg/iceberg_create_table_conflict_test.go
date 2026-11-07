package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	conflictBucket    = "warehouse"
	conflictNamespace = "ns"
	conflictTable     = "orders"
	conflictLocation  = "s3://" + conflictBucket + "/" + conflictNamespace + "/" + conflictTable
)

// filerHooks lets a test land a competing write, or a failure, at a chosen RPC
// of the handler under test while the in-memory filer still arbitrates them.
type filerHooks struct {
	// afterCreate runs once the filer has accepted a CreateEntry.
	afterCreate func(*filer_pb.CreateEntryRequest)
	// updateErr fails an UpdateEntry, before it reaches the filer, when it
	// returns an error.
	updateErr func(*filer_pb.UpdateEntryRequest) error
	// afterUpdate runs once the filer has accepted an UpdateEntry.
	afterUpdate func(*filer_pb.UpdateEntryRequest)
	// lookupErr fails a LookupDirectoryEntry, before it reaches the filer,
	// when it returns an error.
	lookupErr func(*filer_pb.LookupDirectoryEntryRequest) error
}

type hookedFiler struct {
	base  *s3tables.ManagerClient
	hooks *filerHooks
}

func (h hookedFiler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return h.base.WithFilerClient(streamingMode, func(client filer_pb.SeaweedFilerClient) error {
		return fn(&hookedClient{SeaweedFilerClient: client, hooks: h.hooks})
	})
}

type hookedClient struct {
	filer_pb.SeaweedFilerClient
	hooks *filerHooks
}

func (c *hookedClient) CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.CreateEntry(ctx, in, opts...)
	if err == nil && resp.GetErrorCode() == filer_pb.FilerError_OK && c.hooks.afterCreate != nil {
		c.hooks.afterCreate(in)
	}
	return resp, err
}

func (c *hookedClient) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	if c.hooks.updateErr != nil {
		if err := c.hooks.updateErr(in); err != nil {
			return nil, err
		}
	}
	resp, err := c.SeaweedFilerClient.UpdateEntry(ctx, in, opts...)
	if err == nil && c.hooks.afterUpdate != nil {
		c.hooks.afterUpdate(in)
	}
	return resp, err
}

func (c *hookedClient) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if c.hooks.lookupErr != nil {
		if err := c.hooks.lookupErr(in); err != nil {
			return nil, err
		}
	}
	return c.SeaweedFilerClient.LookupDirectoryEntry(ctx, in, opts...)
}

// conflictEnv is a catalog over the in-memory filer, which honours O_EXCL and
// the compare-checked updates the real filer's per-path lock provides, and is
// safe for the concurrent creators these tests race against each other.
type conflictEnv struct {
	t      *testing.T
	filer  *s3tablestest.MemFiler
	server *Server
	router *mux.Router
	admin  *s3tables.Manager
	arn    string
	hooks  *filerHooks
}

func newConflictEnv(t *testing.T) *conflictEnv {
	t.Helper()
	filer := s3tablestest.Start(t)
	hooks := &filerHooks{}
	server := NewServer(hookedFiler{base: s3tables.NewManagerClient(filer.Client), hooks: hooks}, openAuthenticator{})
	router := mux.NewRouter().SkipClean(true)
	server.RegisterRoutes(router)

	admin := s3tables.NewManager()
	admin.SetTrusted(true)
	arn, err := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3tables.DefaultAccountID, conflictBucket)
	if err != nil {
		t.Fatal(err)
	}
	e := &conflictEnv{t: t, filer: filer, server: server, router: router, admin: admin, arn: arn, hooks: hooks}
	e.adminDo("CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: conflictBucket, Format: s3tables.FormatIceberg}, nil)
	e.adminDo("CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{conflictNamespace}}, nil)
	return e
}

func (e *conflictEnv) adminDo(op string, req, resp interface{}) {
	e.t.Helper()
	if err := e.admin.Execute(e.t.Context(), s3tables.NewManagerClient(e.filer.Client), op, req, resp, ""); err != nil {
		e.t.Fatalf("%s: %v", op, err)
	}
}

// createTable posts a CreateTable body and returns the response.
func (e *conflictEnv) createTable(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/"+conflictBucket+"/namespaces/"+conflictNamespace+"/tables", strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	return rec
}

func (e *conflictEnv) record() (*s3tables.GetTableResponse, error) {
	var resp s3tables.GetTableResponse
	err := e.admin.Execute(e.t.Context(), s3tables.NewManagerClient(e.filer.Client), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: e.arn, Namespace: []string{conflictNamespace}, Name: conflictTable,
	}, &resp, "")
	return &resp, err
}

func (e *conflictEnv) mustRecord() *s3tables.GetTableResponse {
	e.t.Helper()
	resp, err := e.record()
	if err != nil {
		e.t.Fatalf("GetTable: %v", err)
	}
	return resp
}

func (e *conflictEnv) metadataDir() string {
	return metadataDirPath(conflictBucket, path.Join(conflictNamespace, conflictTable))
}

// metadataFiles lists the *.metadata.json files in the table's metadata directory.
func (e *conflictEnv) metadataFiles() []string {
	e.t.Helper()
	var names []string
	err := filer_pb.SeaweedList(e.t.Context(), e.filer.Client, e.metadataDir(), "", func(entry *filer_pb.Entry, _ bool) error {
		if strings.HasSuffix(entry.Name, ".metadata.json") {
			names = append(names, entry.Name)
		}
		return nil
	}, "", false, 0)
	if err != nil {
		e.t.Fatalf("list %s: %v", e.metadataDir(), err)
	}
	sort.Strings(names)
	return names
}

// metadataFile returns the Iceberg metadata a metadata-location names, read
// straight from the filer the way a client reading S3 directly would.
func (e *conflictEnv) metadataFile(location string) table.Metadata {
	e.t.Helper()
	entry := e.filer.Get(e.metadataDir(), path.Base(location))
	if entry == nil {
		e.t.Fatalf("no metadata file behind %s", location)
	}
	metadata, err := table.ParseMetadataBytes(entry.Content)
	if err != nil {
		e.t.Fatalf("parse %s: %v", location, err)
	}
	return metadata
}

// snapshot captures everything under the bucket: every entry's content and
// attributes, which is where the catalog record and its staged files live.
func (e *conflictEnv) snapshot() map[string]string {
	e.t.Helper()
	out := map[string]string{}
	var walk func(dir string)
	walk = func(dir string) {
		var children []string
		err := filer_pb.SeaweedList(e.t.Context(), e.filer.Client, dir, "", func(entry *filer_pb.Entry, _ bool) error {
			keys := make([]string, 0, len(entry.Extended))
			for key := range entry.Extended {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			var sb strings.Builder
			fmt.Fprintf(&sb, "dir=%v content=%q", entry.IsDirectory, entry.Content)
			for _, key := range keys {
				fmt.Fprintf(&sb, " %s=%q", key, entry.Extended[key])
			}
			p := path.Join(dir, entry.Name)
			out[p] = sb.String()
			if entry.IsDirectory {
				children = append(children, p)
			}
			return nil
		}, "", false, 0)
		if err != nil {
			e.t.Fatalf("list %s: %v", dir, err)
		}
		for _, child := range children {
			walk(child)
		}
	}
	walk(path.Join(s3tables.TablesPath, conflictBucket))
	return out
}

func assertSnapshotUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	for p, want := range before {
		got, ok := after[p]
		if !ok {
			t.Errorf("%s was removed", p)
		} else if got != want {
			t.Errorf("%s changed:\n before %s\n after  %s", p, want, got)
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s was added", p)
		}
	}
}

// conflictTableBody is a CreateTable body whose schema carries column, so the
// table a request produced can be told from another's.
func conflictTableBody(column string, extra string) string {
	schema := `{"type":"struct","schema-id":0,"fields":[` +
		`{"id":1,"name":"id","required":false,"type":"long"},` +
		`{"id":2,"name":"` + column + `","required":false,"type":"string"}]}`
	body := `{"name":"` + conflictTable + `","schema":` + schema
	if extra != "" {
		body += "," + extra
	}
	return body + "}"
}

func decodeIcebergError(t *testing.T, rec *httptest.ResponseRecorder) ErrorModel {
	t.Helper()
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return resp.Error
}

func metadataLocationOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var resp struct {
		MetadataLocation string `json:"metadata-location"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode load result %q: %v", rec.Body.String(), err)
	}
	return resp.MetadataLocation
}

func hasColumn(metadata table.Metadata, column string) bool {
	_, ok := metadata.CurrentSchema().FindFieldByName(column)
	return ok
}

// Creating a table that exists is a 409 in the Iceberg REST spec, whatever
// schema the second request carries. The first table must not move.
func TestCreateTableOnExistingTableIsConflict(t *testing.T) {
	e := newConflictEnv(t)

	first := e.createTable(conflictTableBody("first_col", ""))
	if first.Code != http.StatusOK {
		t.Fatalf("first create = %d (%s)", first.Code, first.Body.String())
	}
	record := e.mustRecord()
	before := e.snapshot()

	second := e.createTable(conflictTableBody("second_col", ""))
	if second.Code != http.StatusConflict {
		t.Fatalf("second create = %d (%s), want 409", second.Code, second.Body.String())
	}
	if got := decodeIcebergError(t, second); got.Type != "AlreadyExistsException" || got.Code != http.StatusConflict {
		t.Errorf("error = %+v, want AlreadyExistsException/409", got)
	}

	assertSnapshotUnchanged(t, before, e.snapshot())
	after := e.mustRecord()
	if after.MetadataLocation != record.MetadataLocation || after.VersionToken != record.VersionToken {
		t.Errorf("record moved from %s/%s to %s/%s", record.MetadataLocation, record.VersionToken, after.MetadataLocation, after.VersionToken)
	}
	if got := e.metadataFile(after.MetadataLocation); !hasColumn(got, "first_col") || hasColumn(got, "second_col") {
		t.Error("the first table's metadata file was replaced")
	}
}

// Stage-create on a name that is taken is refused at once, rather than after
// the engine has written its data: nothing is staged and no marker is left.
func TestStageCreateOnExistingTableIsConflict(t *testing.T) {
	e := newConflictEnv(t)
	if rec := e.createTable(conflictTableBody("first_col", "")); rec.Code != http.StatusOK {
		t.Fatalf("create = %d (%s)", rec.Code, rec.Body.String())
	}
	before := e.snapshot()

	rec := e.createTable(conflictTableBody("second_col", `"stage-create":true`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("stage-create = %d (%s), want 409", rec.Code, rec.Body.String())
	}
	if got := decodeIcebergError(t, rec); got.Type != "AlreadyExistsException" {
		t.Errorf("error type = %q, want AlreadyExistsException", got.Type)
	}
	assertSnapshotUnchanged(t, before, e.snapshot())
}

// A caller may name a location that already holds a metadata file the catalog
// does not know about, whether a table's leftover or another table's. The file
// stays as it is and the new table gets a name of its own beside it.
func TestCreateTableKeepsUnregisteredMetadataFile(t *testing.T) {
	e := newConflictEnv(t)
	seed := []byte(`{"not":"this table"}`)
	if err := e.server.saveMetadataFile(t.Context(), conflictBucket, path.Join(conflictNamespace, conflictTable), "v1.metadata.json", seed, true); err != nil {
		t.Fatal(err)
	}

	rec := e.createTable(conflictTableBody("col", `"location":"`+conflictLocation+`"`))
	if rec.Code != http.StatusOK {
		t.Fatalf("create = %d (%s)", rec.Code, rec.Body.String())
	}

	got := metadataLocationOf(t, rec)
	if !regexp.MustCompile(`^` + regexp.QuoteMeta(conflictLocation) + `/metadata/v1-[0-9a-f-]{36}\.metadata\.json$`).MatchString(got) {
		t.Errorf("metadata-location = %s, want %s/metadata/v1-<uuid>.metadata.json", got, conflictLocation)
	}
	if kept := e.filer.Get(e.metadataDir(), "v1.metadata.json"); kept == nil || string(kept.Content) != string(seed) {
		t.Errorf("the file already at v1.metadata.json was replaced: %+v", kept)
	}
	record := e.mustRecord()
	if record.MetadataLocation != got {
		t.Errorf("catalog points at %s, response said %s", record.MetadataLocation, got)
	}
	if record.MetadataVersion != 1 {
		t.Errorf("catalog version = %d, want 1", record.MetadataVersion)
	}
	if !hasColumn(e.metadataFile(record.MetadataLocation), "col") {
		t.Error("the file the catalog points at is not this table's metadata")
	}
}

// Many creators racing for one name: exactly one wins, and the metadata file
// the catalog points at is the winner's own, whichever name each tried first.
// The losers must not leave files behind or take the winner's.
func TestConcurrentCreateTableHasOneWinner(t *testing.T) {
	const creators = 8
	e := newConflictEnv(t)

	codes := make([]int, creators)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(creators)
	done.Add(creators)
	for i := range creators {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			rec := e.createTable(conflictTableBody(fmt.Sprintf("col_%d", i), `"location":"`+conflictLocation+`"`))
			codes[i] = rec.Code
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	winner := -1
	for i, code := range codes {
		switch code {
		case http.StatusOK:
			if winner != -1 {
				t.Fatalf("creators %d and %d both got 200: %v", winner, i, codes)
			}
			winner = i
		case http.StatusConflict:
		default:
			t.Errorf("creator %d got %d, want 200 or 409: %v", i, code, codes)
		}
	}
	if winner == -1 {
		t.Fatalf("no creator won: %v", codes)
	}

	record := e.mustRecord()
	metadata := e.metadataFile(record.MetadataLocation)
	if record.Metadata == nil || record.Metadata.Iceberg == nil {
		t.Fatalf("record carries no Iceberg metadata: %+v", record)
	}
	if got, want := metadata.TableUUID().String(), record.Metadata.Iceberg.TableUUID; got != want {
		t.Errorf("file behind %s has table-uuid %s, catalog says %s", record.MetadataLocation, got, want)
	}
	for i := range creators {
		if has := hasColumn(metadata, fmt.Sprintf("col_%d", i)); has != (i == winner) {
			t.Errorf("column col_%d present = %v, but creator %d won", i, has, winner)
		}
	}
	if files := e.metadataFiles(); len(files) != 1 || files[0] != path.Base(record.MetadataLocation) {
		t.Errorf("metadata files = %v, want only %s", files, path.Base(record.MetadataLocation))
	}
}

// A creator whose request reaches the catalog after another's has registered
// the name is answered with that table by the catalog, not with an error. It
// is still the loser: 409, and the file it staged goes, not the winner's. The
// S3 Tables API registers a table without a metadata location, and the
// catalog then answers with an empty one.
func TestCreateTableDiscardsFileWhenNameWasTakenMidFlight(t *testing.T) {
	for name, winnerLocation := range map[string]string{
		"winner has a location": conflictLocation + "/metadata/v1-winner.metadata.json",
		"winner has none":       "",
	} {
		t.Run(name, func(t *testing.T) {
			e := newConflictEnv(t)
			winnerUUID := uuid.NewString()
			registered := false
			e.hooks.afterCreate = func(in *filer_pb.CreateEntryRequest) {
				if registered || in.Entry.IsDirectory || !strings.HasSuffix(in.Entry.Name, ".metadata.json") {
					return
				}
				registered = true
				e.registerWinner(winnerLocation, winnerUUID)
			}

			rec := e.createTable(conflictTableBody("col", `"location":"`+conflictLocation+`"`))
			if !registered {
				t.Fatal("the competing registration never ran")
			}
			if rec.Code != http.StatusConflict {
				t.Fatalf("create = %d (%s), want 409", rec.Code, rec.Body.String())
			}
			if files := e.metadataFiles(); len(files) != 0 {
				t.Errorf("the losing request left %v behind", files)
			}
			e.assertWinnerRecord(winnerLocation, winnerUUID)
		})
	}
}

// registerWinner registers the shared test table through the S3 Tables API,
// the way a competing creator would.
func (e *conflictEnv) registerWinner(metadataLocation, tableUUID string) {
	e.t.Helper()
	e.adminDo("CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: e.arn, Namespace: []string{conflictNamespace}, Name: conflictTable, Format: s3tables.FormatIceberg,
		MetadataLocation: metadataLocation, MetadataVersion: 1,
		Metadata: &s3tables.TableMetadata{Iceberg: &s3tables.IcebergMetadata{TableUUID: tableUUID}},
	}, nil)
}

func (e *conflictEnv) assertWinnerRecord(metadataLocation, tableUUID string) {
	e.t.Helper()
	record := e.mustRecord()
	if record.MetadataLocation != metadataLocation {
		e.t.Errorf("catalog points at %q, want the winner's %q", record.MetadataLocation, metadataLocation)
	}
	if record.Metadata == nil || record.Metadata.Iceberg == nil || record.Metadata.Iceberg.TableUUID != tableUUID {
		e.t.Errorf("catalog record %+v, want the winner's table-uuid %s", record.Metadata, tableUUID)
	}
}

// The staged file goes when the catalog refuses the registration outright, and
// stays when the outcome is unknown: the record may have been written, and a
// catalog pointing at a deleted file cannot be read.
func TestCreateTableStagedFileOnRegistrationFailure(t *testing.T) {
	t.Run("transport failure keeps the file", func(t *testing.T) {
		e := newConflictEnv(t)
		e.hooks.updateErr = func(in *filer_pb.UpdateEntryRequest) error {
			if in.Entry.Name == conflictTable {
				return status.Error(codes.Unavailable, "filer unavailable")
			}
			return nil
		}

		rec := e.createTable(conflictTableBody("col", `"location":"`+conflictLocation+`"`))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("create = %d (%s), want 500", rec.Code, rec.Body.String())
		}
		if files := e.metadataFiles(); len(files) != 1 {
			t.Errorf("metadata files = %v, want the staged file kept", files)
		}
	})

	t.Run("refusal removes the file", func(t *testing.T) {
		e := newConflictEnv(t)
		revoked := false
		e.hooks.afterCreate = func(in *filer_pb.CreateEntryRequest) {
			if revoked || in.Entry.IsDirectory || !strings.HasSuffix(in.Entry.Name, ".metadata.json") {
				return
			}
			revoked = true
			// The namespace is gone by the time the catalog registers the table.
			e.adminDo("DeleteNamespace", &s3tables.DeleteNamespaceRequest{TableBucketARN: e.arn, Namespace: []string{conflictNamespace}}, nil)
		}

		rec := e.createTable(conflictTableBody("col", `"location":"`+conflictLocation+`"`))
		if !revoked {
			t.Fatal("the namespace was never removed")
		}
		if rec.Code < 400 || rec.Code >= 500 {
			t.Fatalf("create = %d (%s), want a 4xx", rec.Code, rec.Body.String())
		}
		if files := e.metadataFiles(); len(files) != 0 {
			t.Errorf("metadata files = %v, want the refused file removed", files)
		}
	})
}

// armSurvivingConflict makes the next create of the test table end in a
// conflict that leaves its record in place: once the catalog confirms the
// record, another caller updates it without moving its metadata location, and
// the namespace recheck that follows fails. The catalog then cannot back the
// changed record out. It reports whether the update ran.
func (e *conflictEnv) armSurvivingConflict() *bool {
	namespacePath := s3tables.GetNamespacePath(conflictBucket, conflictNamespace)
	updated, failRecheck := false, false
	e.hooks.afterUpdate = func(in *filer_pb.UpdateEntryRequest) {
		if _, marked := in.Entry.Extended[s3tables.ExtendedKeyDeleting]; updated || marked || in.Entry.Name != conflictTable {
			return
		}
		updated = true
		e.adminDo("UpdateTable", &s3tables.UpdateTableRequest{
			TableBucketARN: e.arn, Namespace: []string{conflictNamespace}, Name: conflictTable,
		}, nil)
		failRecheck = true
	}
	e.hooks.lookupErr = func(in *filer_pb.LookupDirectoryEntryRequest) error {
		if failRecheck && path.Join(in.Directory, in.Name) == namespacePath {
			failRecheck = false
			return status.Error(codes.Unavailable, "filer unavailable")
		}
		return nil
	}
	return &updated
}

// A conflict does not always mean the create left nothing behind: the record
// it could not back out still points at the staged file, so the file stays.
func TestCreateKeepsFileOfRecordThatSurvivedConflict(t *testing.T) {
	for name, create := range map[string]func(e *conflictEnv) (int, string){
		"create table": func(e *conflictEnv) (int, string) {
			rec := e.createTable(conflictTableBody("col", `"location":"`+conflictLocation+`"`))
			if rec.Code == http.StatusOK {
				return rec.Code, ""
			}
			return rec.Code, decodeIcebergError(t, rec).Type
		},
		"create on commit": func(e *conflictEnv) (int, string) {
			_, reqErr := e.server.finalizeCreateOnCommit(t.Context(), e.finalizeInput(uuid.New()))
			if reqErr == nil {
				return http.StatusOK, ""
			}
			return reqErr.status, reqErr.errType
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := newConflictEnv(t)
			updated := e.armSurvivingConflict()

			code, errType := create(e)
			if !*updated {
				t.Fatal("the competing update never ran")
			}
			if code != http.StatusConflict || errType != "CommitFailedException" {
				t.Fatalf("create = %d %s, want the catalog's 409 CommitFailedException", code, errType)
			}
			record := e.mustRecord()
			if want := conflictLocation + "/metadata/v1.metadata.json"; record.MetadataLocation != want {
				t.Fatalf("surviving record points at %s, want the staged %s", record.MetadataLocation, want)
			}
			if files := e.metadataFiles(); len(files) != 1 || files[0] != path.Base(record.MetadataLocation) {
				t.Errorf("metadata files = %v, want the surviving record's %s kept", files, path.Base(record.MetadataLocation))
			}
		})
	}
}

func TestRecordReferencesAt(t *testing.T) {
	const staged = conflictLocation + "/metadata/v1.metadata.json"
	stagedUUID := uuid.NewString()
	tablePath := s3tables.GetTablePath(conflictBucket, conflictNamespace, conflictTable)
	putEntry := func(e *conflictEnv, extended map[string][]byte) {
		e.filer.Put(path.Dir(tablePath), path.Base(tablePath), extended)
	}
	for name, tc := range map[string]struct {
		setup func(e *conflictEnv)
		want  bool
	}{
		"no record":                  {func(e *conflictEnv) {}, false},
		"directory without a record": {func(e *conflictEnv) { putEntry(e, nil) }, false},
		"another table's record": {func(e *conflictEnv) {
			e.registerWinner(conflictLocation+"/metadata/v1-other.metadata.json", uuid.NewString())
		}, false},
		"record names the file":      {func(e *conflictEnv) { e.registerWinner(staged, uuid.NewString()) }, true},
		"record of this table moved": {func(e *conflictEnv) { e.registerWinner(conflictLocation+"/metadata/v2.metadata.json", stagedUUID) }, true},
		"record still marked by an unfinished create": {func(e *conflictEnv) {
			putEntry(e, map[string][]byte{
				s3tables.ExtendedKeyMetadata: []byte(`{"metadataLocation":"` + staged + `"}`),
				s3tables.ExtendedKeyDeleting: []byte("create"),
			})
		}, true},
		"record undecodable": {func(e *conflictEnv) {
			putEntry(e, map[string][]byte{s3tables.ExtendedKeyMetadata: []byte("{")})
		}, true},
		"record unreadable": {func(e *conflictEnv) {
			e.hooks.lookupErr = func(in *filer_pb.LookupDirectoryEntryRequest) error {
				if path.Join(in.Directory, in.Name) == tablePath {
					return status.Error(codes.Unavailable, "filer unavailable")
				}
				return nil
			}
		}, true},
	} {
		t.Run(name, func(t *testing.T) {
			e := newConflictEnv(t)
			tc.setup(e)
			if got := e.server.recordReferencesAt(t.Context(), conflictBucket, []string{conflictNamespace}, conflictTable, staged, stagedUUID); got != tc.want {
				t.Errorf("recordReferencesAt = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegistrationRefused(t *testing.T) {
	typed := func(code string) error { return &s3tables.S3TablesError{Type: code, Message: code} }
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"name taken":           {typed(s3tables.ErrCodeTableAlreadyExists), true},
		"conflict":             {typed(s3tables.ErrCodeConflict), true},
		"version mismatch":     {s3tables.ErrVersionTokenMismatch, true},
		"access denied":        {typed(s3tables.ErrCodeAccessDenied), true},
		"no such namespace":    {typed(s3tables.ErrCodeNoSuchNamespace), true},
		"invalid request":      {typed(s3tables.ErrCodeInvalidRequest), true},
		"internal error":       {typed(s3tables.ErrCodeInternalError), false},
		"transport error":      {errors.New("rpc error: code = Unavailable desc = filer unavailable"), false},
		"wrapped name taken":   {fmt.Errorf("create: %w", typed(s3tables.ErrCodeTableAlreadyExists)), true},
		"untyped name taken":   {errors.New("table orders already exists"), true},
		"wrapped server fault": {fmt.Errorf("create: %w", typed(s3tables.ErrCodeInternalError)), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := registrationRefused(tc.err); got != tc.want {
				t.Errorf("registrationRefused(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// finalizeInput is a create-on-commit for the shared test table, as one of
// several writers racing to create it would issue.
func (e *conflictEnv) finalizeInput(tableUUID uuid.UUID) createOnCommitInput {
	e.t.Helper()
	base, err := newTableMetadata(tableUUID, conflictLocation, nil, nil, nil, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	return createOnCommitInput{
		bucketARN:    e.arn,
		markerBucket: conflictBucket,
		namespace:    []string{conflictNamespace},
		tableName:    conflictTable,
		identityName: s3tables.DefaultAccountID,
		location:     conflictLocation,
		tableUUID:    tableUUID,
		baseMetadata: base,
	}
}

// A create-on-commit that loses the race must remove its own metadata file,
// not the v1.metadata.json the winner's catalog record points at.
func TestFinalizeCreateOnCommitLosesToExistingTable(t *testing.T) {
	e := newConflictEnv(t)
	if rec := e.createTable(conflictTableBody("winner_col", `"location":"`+conflictLocation+`"`)); rec.Code != http.StatusOK {
		t.Fatalf("create = %d (%s)", rec.Code, rec.Body.String())
	}
	record := e.mustRecord()
	if base := path.Base(record.MetadataLocation); base != "v1.metadata.json" {
		t.Fatalf("winner's file = %s, want v1.metadata.json", base)
	}
	before := e.snapshot()

	_, reqErr := e.server.finalizeCreateOnCommit(t.Context(), e.finalizeInput(uuid.New()))
	if reqErr == nil || reqErr.status != http.StatusConflict || reqErr.errType != "CommitFailedException" {
		t.Fatalf("finalize = %+v, want 409 CommitFailedException", reqErr)
	}

	assertSnapshotUnchanged(t, before, e.snapshot())
	if after := e.mustRecord(); after.MetadataLocation != record.MetadataLocation {
		t.Errorf("catalog moved from %s to %s", record.MetadataLocation, after.MetadataLocation)
	}
}

// The winner may be a table registered through the S3 Tables API without a
// metadata location, which the catalog answers with an empty one. The commit
// still lost: it must not report the table as created by its own metadata.
func TestFinalizeCreateOnCommitLosesToTableWithoutLocation(t *testing.T) {
	e := newConflictEnv(t)
	winnerUUID := uuid.NewString()
	e.registerWinner("", winnerUUID)

	_, reqErr := e.server.finalizeCreateOnCommit(t.Context(), e.finalizeInput(uuid.New()))
	if reqErr == nil || reqErr.status != http.StatusConflict || reqErr.errType != "CommitFailedException" {
		t.Fatalf("finalize = %+v, want 409 CommitFailedException", reqErr)
	}
	if files := e.metadataFiles(); len(files) != 0 {
		t.Errorf("the losing commit left %v behind", files)
	}
	e.assertWinnerRecord("", winnerUUID)
}

// Two stage-create commits for one name: one creates the table and the other is
// told it lost, and the file the catalog points at belongs to the winner.
func TestConcurrentFinalizeCreateOnCommitHasOneWinner(t *testing.T) {
	const creators = 8
	e := newConflictEnv(t)

	uuids := make([]uuid.UUID, creators)
	results := make([]*icebergRequestError, creators)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(creators)
	done.Add(creators)
	for i := range creators {
		uuids[i] = uuid.New()
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, results[i] = e.server.finalizeCreateOnCommit(t.Context(), e.finalizeInput(uuids[i]))
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	winner := -1
	for i, reqErr := range results {
		switch {
		case reqErr == nil:
			if winner != -1 {
				t.Fatalf("creators %d and %d both created the table", winner, i)
			}
			winner = i
		case reqErr.status != http.StatusConflict || reqErr.errType != "CommitFailedException":
			t.Errorf("creator %d: %+v, want 409 CommitFailedException", i, reqErr)
		}
	}
	if winner == -1 {
		t.Fatal("no creator won")
	}

	record := e.mustRecord()
	if got := e.metadataFile(record.MetadataLocation).TableUUID(); got != uuids[winner] {
		t.Errorf("file behind %s has table-uuid %s, want the winner's %s", record.MetadataLocation, got, uuids[winner])
	}
	if got := record.Metadata.Iceberg.TableUUID; got != uuids[winner].String() {
		t.Errorf("catalog table-uuid = %s, want the winner's %s", got, uuids[winner])
	}
	if files := e.metadataFiles(); len(files) != 1 || files[0] != path.Base(record.MetadataLocation) {
		t.Errorf("metadata files = %v, want only %s", files, path.Base(record.MetadataLocation))
	}
}
