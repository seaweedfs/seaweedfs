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

func TestListSqlDefaultOrderingFollowsColumn(t *testing.T) {
	gen := &SqlGenPostgres{}
	for _, got := range []string{gen.GetSqlListExclusive("filemeta"), gen.GetSqlListInclusive("filemeta")} {
		if strings.Contains(got, "COLLATE") {
			t.Fatalf("default list query should not force a collation, got: %s", got)
		}
		if !strings.Contains(got, "ORDER BY name ASC") {
			t.Fatalf("expected plain name ordering, got: %s", got)
		}
	}
}

func TestListSqlBinaryOrderingOnNonBinaryColumn(t *testing.T) {
	gen := &SqlGenPostgres{ForceBinaryCollation: true}
	for _, got := range []string{gen.GetSqlListExclusive("filemeta"), gen.GetSqlListInclusive("filemeta")} {
		if !strings.Contains(got, `ORDER BY name COLLATE "C" ASC`) {
			t.Fatalf(`expected COLLATE "C" ordering, got: %s`, got)
		}
		if !strings.Contains(got, `name COLLATE "C" like $4`) {
			t.Fatalf(`expected COLLATE "C" prefix filter, got: %s`, got)
		}
		if strings.Contains(got, "AND name>$2") || strings.Contains(got, "AND name>=$2") {
			t.Fatalf("pagination comparison must also be byte-ordered, got: %s", got)
		}
	}
}

func TestIsByteOrderedCollation(t *testing.T) {
	byteOrdered := []string{"C", "POSIX", "c", "C.UTF-8", "C.utf8"}
	for _, c := range byteOrdered {
		if !isByteOrderedCollation(c) {
			t.Fatalf("expected %q to be treated as byte-ordered", c)
		}
	}
	locale := []string{"en_US.UTF-8", "en_US.utf8", "en-US-x-icu", "und-x-icu", ""}
	for _, c := range locale {
		if isByteOrderedCollation(c) {
			t.Fatalf("expected %q to be treated as locale-aware", c)
		}
	}
}
