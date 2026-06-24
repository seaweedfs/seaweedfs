package s3tables

import "testing"

func TestEntryType(t *testing.T) {
	cases := []struct {
		name     string
		extended map[string][]byte
		want     string
	}{
		{"nil extended is table", nil, EntryTypeTable},
		{"absent marker is table", map[string][]byte{ExtendedKeyMetadata: []byte("{}")}, EntryTypeTable},
		{"empty marker is table", map[string][]byte{ExtendedKeyEntryType: {}}, EntryTypeTable},
		{"table marker", map[string][]byte{ExtendedKeyEntryType: []byte(EntryTypeTable)}, EntryTypeTable},
		{"view marker", map[string][]byte{ExtendedKeyEntryType: []byte(EntryTypeView)}, EntryTypeView},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := entryType(c.extended); got != c.want {
				t.Fatalf("entryType() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestGenerateViewARN(t *testing.T) {
	h := NewS3TablesHandler()
	got := h.generateViewARN(DefaultAccountID, "warehouse", "ns/v")
	want := "arn:aws:s3tables:us-east-1:000000000000:bucket/warehouse/view/ns/v"
	if got != want {
		t.Fatalf("generateViewARN() = %q, want %q", got, want)
	}
}
