package lance

import "testing"

func TestParseIdentifier(t *testing.T) {
	cases := []struct {
		name      string
		encoded   string
		delimiter string
		want      []string
		wantErr   bool
	}{
		{name: "root is the delimiter", encoded: "$", delimiter: "$"},
		{name: "empty is the root", encoded: "", delimiter: "$"},
		{name: "bucket", encoded: "analytics", delimiter: "$", want: []string{"analytics"}},
		{name: "namespace", encoded: "analytics$sales", delimiter: "$", want: []string{"analytics", "sales"}},
		{name: "table", encoded: "analytics$sales$orders", delimiter: "$", want: []string{"analytics", "sales", "orders"}},
		{name: "custom delimiter", encoded: "a.b.c", delimiter: ".", want: []string{"a", "b", "c"}},
		{name: "empty part is rejected", encoded: "a$$b", delimiter: "$", wantErr: true},
		{name: "trailing delimiter is rejected", encoded: "a$", delimiter: "$", wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseIdentifier(c.encoded, c.delimiter)
			if c.wantErr {
				if err == nil {
					t.Fatalf("parseIdentifier(%q) error = nil, want an error", c.encoded)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseIdentifier(%q) error = %v", c.encoded, err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("parseIdentifier(%q) = %v, want %v", c.encoded, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("parseIdentifier(%q) = %v, want %v", c.encoded, got, c.want)
				}
			}
		})
	}
}

func TestIdentifierNamespace(t *testing.T) {
	bucket, ns := identifier(nil).namespace()
	if bucket != "" || len(ns) != 0 {
		t.Fatalf("root namespace() = %q %v, want empty", bucket, ns)
	}

	bucket, ns = identifier{"analytics"}.namespace()
	if bucket != "analytics" || len(ns) != 0 {
		t.Fatalf("bucket namespace() = %q %v", bucket, ns)
	}

	bucket, ns = identifier{"analytics", "sales", "eu"}.namespace()
	if bucket != "analytics" || len(ns) != 2 || ns[0] != "sales" || ns[1] != "eu" {
		t.Fatalf("nested namespace() = %q %v", bucket, ns)
	}
}

// A table always needs a bucket, a namespace and a name: storage has no
// unnamespaced tables, so a two-part identifier is a client error rather than a
// table at the top of a bucket.
func TestIdentifierTable(t *testing.T) {
	if _, _, _, err := (identifier{"analytics", "orders"}).table(); err == nil {
		t.Fatal("table() on a two-part identifier error = nil, want an error")
	}

	bucket, ns, name, err := identifier{"analytics", "sales", "eu", "orders"}.table()
	if err != nil {
		t.Fatalf("table() error = %v", err)
	}
	if bucket != "analytics" || name != "orders" || len(ns) != 2 || ns[0] != "sales" || ns[1] != "eu" {
		t.Fatalf("table() = %q %v %q", bucket, ns, name)
	}
}

func TestIdentifierMatchesBody(t *testing.T) {
	id := identifier{"a", "b", "c"}
	if !id.matchesBody(nil) {
		t.Fatal("an absent body identifier must defer to the route")
	}
	if !id.matchesBody([]string{"a", "b", "c"}) {
		t.Fatal("an equal body identifier must match")
	}
	if id.matchesBody([]string{"a", "b"}) {
		t.Fatal("a shorter body identifier must not match")
	}
	if id.matchesBody([]string{"a", "b", "d"}) {
		t.Fatal("a different body identifier must not match")
	}
}

func TestIdentifierString(t *testing.T) {
	if got := identifier(nil).String("$"); got != "$" {
		t.Fatalf("root String() = %q, want %q", got, "$")
	}
	if got := (identifier{"a", "b"}).String("$"); got != "a$b" {
		t.Fatalf("String() = %q, want %q", got, "a$b")
	}
}
