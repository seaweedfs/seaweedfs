package postgres

import (
	"strings"
	"testing"
)

func TestDefaultUpsertQueryIsConflictSafe(t *testing.T) {
	gen := &SqlGenPostgres{UpsertQueryTemplate: DefaultUpsertQuery}
	got := gen.GetSqlInsert("filemeta")
	if !strings.Contains(got, "ON CONFLICT") {
		t.Fatalf("expected ON CONFLICT in default upsert, got: %s", got)
	}
	if !strings.Contains(got, `"filemeta"`) {
		t.Fatalf("expected quoted table name, got: %s", got)
	}
}

func TestEmptyUpsertTemplateFallsBackToPlainInsert(t *testing.T) {
	gen := &SqlGenPostgres{}
	got := gen.GetSqlInsert("filemeta")
	if strings.Contains(got, "ON CONFLICT") {
		t.Fatalf("plain INSERT path should not contain ON CONFLICT, got: %s", got)
	}
}
