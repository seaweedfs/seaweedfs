package iceberg

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
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
			// The filer is empty, so a record holding only a location has no
			// file behind it to serve.
			result, err := (&Server{filerClient: newMemFiler()}).buildLoadTableResult(r, getResp, "bkt", []string{"ns"}, "t")
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

// A caller allowed by the actions on its own identity, rather than by ownership
// or a resource policy, has to survive the hop into the s3tables manager: the
// gate must not refuse what the create behind it would allow.
func TestCreateTableAllowedByIdentityActions(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)

	r := newCreateTableRequest(t, bucket, "finance", `{"name":"quarterly_reports","stage-create":true}`, "mallory")
	r = r.WithContext(s3_constants.SetIdentityInContext(r.Context(), &struct {
		Name    string
		Actions []string
	}{Name: "mallory", Actions: []string{"s3tables:CreateTable"}}))

	w := httptest.NewRecorder()
	s.handleCreateTable(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
	}
}

// Apache Spark numbers the columns of a new table from 0
// and leaves assigning fresh IDs to the catalog. This is the schema Spark
// sent for CREATE TABLE t (id BIGINT, v STRING, ts TIMESTAMP).
const zeroBasedSchema = `{"type":"struct","schema-id":0,"fields":[
	{"id":0,"name":"id","required":false,"type":"long"},
	{"id":1,"name":"v","required":false,"type":"string"},
	{"id":2,"name":"ts","required":false,"type":"timestamptz"}]}`

// The same table as PyIceberg sends it, numbered from 1.
const oneBasedSchema = `{"type":"struct","schema-id":0,"fields":[
	{"id":1,"name":"id","required":false,"type":"long"},
	{"id":2,"name":"v","required":false,"type":"string"},
	{"id":3,"name":"ts","required":false,"type":"timestamptz"}]}`

func createTableBody(name, schema, spec, order string) string {
	parts := []string{`"name":"` + name + `"`, `"schema":` + schema}
	if spec != "" {
		parts = append(parts, `"partition-spec":`+spec)
	}
	if order != "" {
		parts = append(parts, `"write-order":`+order)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

type partitionWant struct{ name, transform, column string }

type sourceIDCase struct {
	name string
	// spec and order reference the zero-based schema; oneBasedSpec and
	// oneBasedOrder are the same definitions against oneBasedSchema.
	spec, order, oneBasedSpec, oneBasedOrder string
	wantPartitions                           []partitionWant
	wantSortColumns                          []string
}

var sourceIDCases = []sourceIDCase{
	{
		// The body Spark sent for PARTITIONED BY (bucket(4, id)).
		name:           "bucket on the first column",
		spec:           `{"spec-id":0,"fields":[{"name":"id_bucket","transform":"bucket[4]","source-id":0,"field-id":1000}]}`,
		oneBasedSpec:   `{"spec-id":0,"fields":[{"name":"id_bucket","transform":"bucket[4]","source-id":1,"field-id":1000}]}`,
		wantPartitions: []partitionWant{{"id_bucket", "bucket[4]", "id"}},
	},
	{
		name: "identity on the first column and day on another",
		spec: `{"spec-id":0,"fields":[{"name":"id","transform":"identity","source-id":0,"field-id":1000},
			{"name":"ts_day","transform":"day","source-id":2,"field-id":1001}]}`,
		oneBasedSpec: `{"spec-id":0,"fields":[{"name":"id","transform":"identity","source-id":1,"field-id":1000},
			{"name":"ts_day","transform":"day","source-id":3,"field-id":1001}]}`,
		wantPartitions: []partitionWant{{"id", "identity", "id"}, {"ts_day", "day", "ts"}},
	},
	{
		name:            "write order on the first column",
		order:           `{"order-id":1,"fields":[{"source-id":0,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}`,
		oneBasedOrder:   `{"order-id":1,"fields":[{"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}`,
		wantSortColumns: []string{"id"},
	},
}

func metadataFromBody(t *testing.T, body string) table.Metadata {
	t.Helper()
	var req CreateTableRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("decode create-table request: %v", err)
	}
	metadata, err := newTableMetadata(uuid.New(), "s3://bkt/ns/t", req.Schema, req.partitionSpec(), req.writeOrder(), req.Properties)
	if err != nil {
		t.Fatalf("newTableMetadata() error = %v", err)
	}
	return metadata
}

func checkResolvedSourceIDs(t *testing.T, metadata table.Metadata, tc sourceIDCase) {
	t.Helper()
	schema := metadata.CurrentSchema()
	if id, ok := schema.FindFieldByName("id"); !ok || id.ID != 1 {
		t.Errorf("first column id = %+v, want fresh field ID 1", id)
	}

	spec := metadata.PartitionSpec()
	var got []partitionWant
	for _, field := range spec.Fields() {
		column, _ := schema.FindColumnName(field.SourceID())
		got = append(got, partitionWant{field.Name, field.Transform.String(), column})
	}
	if len(got) != len(tc.wantPartitions) {
		t.Fatalf("partition fields = %+v, want %+v", got, tc.wantPartitions)
	}
	for i := range got {
		if got[i] != tc.wantPartitions[i] {
			t.Errorf("partition field %d = %+v, want %+v", i, got[i], tc.wantPartitions[i])
		}
	}

	var sortColumns []string
	for _, field := range metadata.SortOrder().Fields() {
		column, _ := schema.FindColumnName(field.SourceIDs[0])
		sortColumns = append(sortColumns, column)
	}
	if strings.Join(sortColumns, ",") != strings.Join(tc.wantSortColumns, ",") {
		t.Errorf("sort columns = %v, want %v", sortColumns, tc.wantSortColumns)
	}
}

// A bound PartitionSpec or SortOrder refuses source-id 0, which rejected every
// Spark CREATE TABLE partitioned or sorted by its first column. newTableMetadata
// resolves the client's placeholders through the request schema, so a
// zero-based request yields the same table as the one-based request PyIceberg
// sends, which the bound types always accepted.
func TestNewTableMetadataResolvesZeroBasedSourceIDs(t *testing.T) {
	marshal := func(t *testing.T, metadata table.Metadata) string {
		t.Helper()
		spec := metadata.PartitionSpec()
		out, err := json.Marshal([]any{metadata.CurrentSchema(), &spec, metadata.SortOrder()})
		if err != nil {
			t.Fatalf("marshal metadata parts: %v", err)
		}
		return string(out)
	}
	for _, tc := range sourceIDCases {
		t.Run(tc.name, func(t *testing.T) {
			zero := metadataFromBody(t, createTableBody("t", zeroBasedSchema, tc.spec, tc.order))
			checkResolvedSourceIDs(t, zero, tc)

			one := metadataFromBody(t, createTableBody("t", oneBasedSchema, tc.oneBasedSpec, tc.oneBasedOrder))
			if got, want := marshal(t, zero), marshal(t, one); got != want {
				t.Errorf("zero-based request produced\n%s\nwant (one-based)\n%s", got, want)
			}
		})
	}
}

// Through the handler, the request Spark sends is accepted and answered with
// the resolved spec.
func TestCreateTablePartitionedByFirstColumn(t *testing.T) {
	const bucket = "warehouse"
	tc := sourceIDCases[1]
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	body := createTableBody("events", zeroBasedSchema, tc.spec, "")
	s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", body, "alice"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
	}

	var result struct {
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	metadata, err := table.ParseMetadataBytes(result.Metadata)
	if err != nil {
		t.Fatalf("parse response metadata: %v", err)
	}
	checkResolvedSourceIDs(t, metadata, tc)
}

// A spec or order naming a column the request schema lacks is the client's
// error: 400, not 500.
func TestCreateTableRejectsUnresolvableSourceID(t *testing.T) {
	const bucket = "warehouse"
	for name, body := range map[string]string{
		"partition spec": createTableBody("orders", zeroBasedSchema,
			`{"spec-id":0,"fields":[{"name":"x","transform":"identity","source-id":42,"field-id":1000}]}`, ""),
		"write order": createTableBody("orders", zeroBasedSchema, "",
			`{"order-id":1,"fields":[{"source-id":42,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, bucket, "finance", "alice")
			s := NewServer(fc, nil)

			w := httptest.NewRecorder()
			s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", body, "alice"))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusBadRequest, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "BadRequestException") {
				t.Errorf("body = %s, want a BadRequestException", w.Body.String())
			}
		})
	}
}
