package iceberg

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestValidateCreateTableRequestRequiresName(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{})
	if !errors.Is(err, errTableNameRequired) {
		t.Fatalf("validateCreateTableRequest() error = %v, want errTableNameRequired", err)
	}
}

func TestValidateCreateTableRequestAcceptsWithName(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("validateCreateTableRequest() error = %v, want nil", err)
	}
}

func TestIsStageCreateEnabledDefaultsToTrue(t *testing.T) {
	t.Setenv("ICEBERG_ENABLE_STAGE_CREATE", "")
	if !isStageCreateEnabled() {
		t.Fatalf("isStageCreateEnabled() = false, want true")
	}
}

func TestIsStageCreateEnabledFalseValues(t *testing.T) {
	falseValues := []string{"0", "false", "FALSE", "no", "off"}
	for _, value := range falseValues {
		t.Setenv("ICEBERG_ENABLE_STAGE_CREATE", value)
		if isStageCreateEnabled() {
			t.Fatalf("isStageCreateEnabled() = true for value %q, want false", value)
		}
	}
}

func mustParseSchema(t *testing.T, raw string) *iceberg.Schema {
	t.Helper()
	var schema iceberg.Schema
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	return &schema
}

const variantSchema = `{"type":"struct","schema-id":0,"fields":[
	{"id":1,"name":"id","required":true,"type":"long"},
	{"id":2,"name":"payload","required":false,"type":"variant"}]}`

// A v3-only column type without format-version 3 is the client's mistake, and
// the reason has to travel back to them rather than only into the server log.
func TestNewTableMetadataRejectsV3TypeBelowV3(t *testing.T) {
	_, err := newTableMetadata(uuid.New(), "s3://bkt/ns/t", mustParseSchema(t, variantSchema), nil, nil, nil)
	if err == nil {
		t.Fatal("newTableMetadata() error = nil, want invalid schema")
	}
	if !errors.Is(err, iceberg.ErrInvalidSchema) {
		t.Fatalf("newTableMetadata() error = %v, want ErrInvalidSchema", err)
	}

	// writeManagerError turns this into a 400; see TestWriteManagerError.
	if !strings.Contains(err.Error(), "variant is not supported until v3") {
		t.Errorf("error = %q, want the underlying reason", err.Error())
	}
}

func TestNewTableMetadataAcceptsV3TypeAtV3(t *testing.T) {
	metadata, err := newTableMetadata(uuid.New(), "s3://bkt/ns/t", mustParseSchema(t, variantSchema), nil, nil,
		iceberg.Properties{"format-version": "3"})
	if err != nil {
		t.Fatalf("newTableMetadata() error = %v, want nil", err)
	}
	if got := metadata.Version(); got != 3 {
		t.Fatalf("metadata.Version() = %d, want 3", got)
	}
	if _, found := metadata.CurrentSchema().FindFieldByName("payload"); !found {
		t.Error("variant field missing from stored schema")
	}
}

// A LoadTable response must never carry nil metadata: it serializes as
// "metadata":null under HTTP 200, which no Iceberg client can parse.
func TestBuildLoadTableResultNeverReturnsNilMetadata(t *testing.T) {
	cases := map[string]s3tables.GetTableResponse{
		"no stored metadata":   {MetadataLocation: "s3://bkt/ns/t/metadata/v1.metadata.json"},
		"empty full metadata":  {Metadata: &s3tables.TableMetadata{}},
		"unparseable metadata": {Metadata: &s3tables.TableMetadata{FullMetadata: json.RawMessage(`{"nope":`)}},
	}
	for name, getResp := range cases {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
			result, err := (&Server{}).buildLoadTableResult(r, getResp, "bkt", []string{"ns"}, "t")
			if err != nil {
				t.Fatalf("buildLoadTableResult() error = %v, want nil", err)
			}
			if result.Metadata == nil {
				t.Fatal("buildLoadTableResult() returned nil metadata with no error")
			}
		})
	}
}

func newCreateTableRequest(t *testing.T, bucket, namespace, body, identity string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/"+namespace+"/tables", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": namespace})
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), identity))
}

// A caller who may not create the table must be refused before anything is
// written: both branches of CreateTable write to the bucket, and stage-create
// never reaches the registration that carries the authorization.
func TestCreateTableDeniedBeforeAnyWrite(t *testing.T) {
	const bucket = "warehouse"
	for name, body := range map[string]string{
		"stage-create": `{"name":"quarterly_reports","stage-create":true}`,
		"create":       `{"name":"quarterly_reports"}`,
	} {
		t.Run(name, func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, bucket, "finance", "alice")
			s := NewServer(fc, nil)

			w := httptest.NewRecorder()
			s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", body, "mallory"))

			if w.Code != http.StatusForbidden {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, http.StatusForbidden, w.Body.String())
			}
			for p, entry := range fc.entries {
				if !entry.IsDirectory && strings.Contains(p, "quarterly_reports") {
					t.Errorf("unauthorized caller wrote %s", p)
				}
			}
		})
	}
}

// The namespace owner still gets the staged metadata and marker stage-create
// exists to leave behind.
func TestStageCreateWritesStagedFilesForOwner(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", `{"name":"quarterly_reports","stage-create":true}`, "alice"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
	}

	staged := 0
	for p, entry := range fc.entries {
		if !entry.IsDirectory && strings.Contains(p, stageCreateMarkerDirName) {
			staged++
		}
	}
	if staged != 2 {
		t.Fatalf("staged files = %d, want the metadata file and its marker", staged)
	}
	if _, ok := fc.entries[s3tables.GetTablePath(bucket, "finance", "quarterly_reports")]; ok {
		t.Error("stage-create registered the table")
	}
}
