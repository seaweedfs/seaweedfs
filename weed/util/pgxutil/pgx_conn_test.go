package pgxutil

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestQuoteDSNValue(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "''"},
		{"plain", "plain"},
		{"with space", "'with space'"},
		{"it's", `'it\'s'`},
		{`back\slash`, `'back\\slash'`},
		{`mix it's \and spaces`, `'mix it\'s \\and spaces'`},
	}
	for _, c := range cases {
		if got := quoteDSNValue(c.in); got != c.want {
			t.Errorf("quoteDSNValue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestBuildDSN_QuotesProblematicValues verifies that the assembled DSN
// stays parseable by pgx when values contain spaces, single quotes or
// backslashes — the kind of password/cert paths a secret manager can
// hand us in production.
func TestBuildDSN_QuotesProblematicValues(t *testing.T) {
	opts := DSNOptions{
		Hostname: "db.example.com",
		Port:     5432,
		User:     "iam user",
		Password: `p@ss it's word\\`,
		Database: "weed",
		Schema:   "public",
		SSLMode:  "disable",
	}
	dsn, adapted := BuildDSN(opts)
	if strings.Contains(adapted, opts.Password) {
		t.Errorf("adapted DSN must redact the password, got %q", adapted)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgx.ParseConfig rejected the assembled DSN: %v\nDSN: %s", err, dsn)
	}
	if cfg.User != opts.User {
		t.Errorf("User: got %q want %q", cfg.User, opts.User)
	}
	if cfg.Password != opts.Password {
		t.Errorf("Password: got %q want %q", cfg.Password, opts.Password)
	}
	if cfg.Database != opts.Database {
		t.Errorf("Database: got %q want %q", cfg.Database, opts.Database)
	}
}
