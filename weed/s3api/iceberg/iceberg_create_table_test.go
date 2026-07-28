package iceberg

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
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

	status, errType, message := metadataBuildError(err)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", status, http.StatusBadRequest)
	}
	if errType != "BadRequestException" {
		t.Errorf("errType = %q, want BadRequestException", errType)
	}
	if !strings.Contains(message, "variant is not supported until v3") {
		t.Errorf("message = %q, want the underlying reason", message)
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
			result, err := (&Server{}).buildLoadTableResult(getResp, "bkt", []string{"ns"}, "t")
			if err != nil {
				t.Fatalf("buildLoadTableResult() error = %v, want nil", err)
			}
			if result.Metadata == nil {
				t.Fatal("buildLoadTableResult() returned nil metadata with no error")
			}
		})
	}
}

// Failures with no client input to blame stay 500 and keep their detail.
func TestMetadataBuildErrorDefaultsToServerError(t *testing.T) {
	status, errType, message := metadataBuildError(errors.New("filer unreachable"))
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", status, http.StatusInternalServerError)
	}
	if errType != "InternalServerError" {
		t.Errorf("errType = %q, want InternalServerError", errType)
	}
	if !strings.Contains(message, "filer unreachable") {
		t.Errorf("message = %q, want the underlying reason", message)
	}
}
