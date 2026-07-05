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

func TestDefaultCreateTableQueryRendersValidSql(t *testing.T) {
	gen := &SqlGenMysql{CreateTableSqlTemplate: DefaultCreateTableQuery}
	got := gen.GetSqlCreateTable("filemeta")
	if strings.Contains(got, "%!") {
		t.Fatalf("template rendered to malformed SQL, got: %s", got)
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

func TestListSqlDefaultOrderingFollowsColumn(t *testing.T) {
	gen := &SqlGenMysql{}
	for _, got := range []string{gen.GetSqlListExclusive("filemeta"), gen.GetSqlListInclusive("filemeta")} {
		if strings.Contains(got, "BINARY") {
			t.Fatalf("default list query should not force BINARY, got: %s", got)
		}
		if !strings.Contains(got, "ORDER BY `name` ASC") {
			t.Fatalf("expected plain name ordering, got: %s", got)
		}
	}
}

func TestListSqlBinaryOrderingOnNonBinaryColumn(t *testing.T) {
	gen := &SqlGenMysql{ForceBinaryCollation: true}
	for _, got := range []string{gen.GetSqlListExclusive("filemeta"), gen.GetSqlListInclusive("filemeta")} {
		if !strings.Contains(got, "ORDER BY BINARY `name` ASC") {
			t.Fatalf("expected BINARY ordering, got: %s", got)
		}
		if !strings.Contains(got, "BINARY `name` LIKE ?") {
			t.Fatalf("expected BINARY prefix filter, got: %s", got)
		}
		if strings.Contains(got, "AND `name` > ?") || strings.Contains(got, "AND `name` >= ?") {
			t.Fatalf("pagination comparison must also be BINARY, got: %s", got)
		}
	}
}

func TestIsBinaryCollation(t *testing.T) {
	binary := []string{"", "binary", "utf8mb4_bin", "utf8mb3_bin", "latin1_bin", "UTF8MB4_BIN"}
	for _, c := range binary {
		if !isBinaryCollation(c) {
			t.Fatalf("expected %q to be treated as binary", c)
		}
	}
	ci := []string{"utf8mb4_general_ci", "utf8mb3_general_ci", "utf8mb4_0900_ai_ci", "latin1_swedish_ci"}
	for _, c := range ci {
		if isBinaryCollation(c) {
			t.Fatalf("expected %q to be treated as non-binary", c)
		}
	}
}
