package postgres

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestConfigureListOrderingSkipsCockroachDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").WillReturnRows(
		sqlmock.NewRows([]string{"version"}).AddRow("CockroachDB CCL v22.1.22 (x86_64-pc-linux-gnu)"))

	gen := &SqlGenPostgres{}
	ConfigureListOrdering(db, gen)
	if gen.ForceBinaryCollation {
		t.Fatal("CockroachDB default ordering is byte-ordered; COLLATE \"C\" must not be forced")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConfigureListOrderingForcesBinaryOnLocalePostgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").WillReturnRows(
		sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2 on x86_64-pc-linux-gnu"))
	mock.ExpectQuery("datcollate").WillReturnRows(
		sqlmock.NewRows([]string{"collation_name", "datcollate"}).AddRow(nil, "en_US.UTF-8"))

	gen := &SqlGenPostgres{}
	ConfigureListOrdering(db, gen)
	if !gen.ForceBinaryCollation {
		t.Fatal("locale-aware Postgres collation must force byte ordering")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
