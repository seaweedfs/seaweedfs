package mysql

import (
	"strings"
	"testing"
)

func TestDefaultUpsertQueryUsesOnDuplicateKey(t *testing.T) {
	gen := &SqlGenMysql{UpsertQueryTemplate: DefaultUpsertQuery}
	got := gen.GetSqlInsert("filemeta")
	if !strings.Contains(got, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("expected ON DUPLICATE KEY UPDATE in default upsert, got: %s", got)
	}
	if strings.Contains(got, "AS `new`") {
		t.Fatalf("default should avoid MySQL 8.0.19 row-alias syntax for MariaDB compat, got: %s", got)
	}
	if !strings.Contains(got, "`filemeta`") {
		t.Fatalf("expected backticked table name, got: %s", got)
	}
}

func TestEmptyUpsertTemplateFallsBackToPlainInsert(t *testing.T) {
	gen := &SqlGenMysql{}
	got := gen.GetSqlInsert("filemeta")
	if strings.Contains(got, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("plain INSERT path should not contain ON DUPLICATE KEY UPDATE, got: %s", got)
	}
}
