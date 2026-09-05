package s3api

import (
	"strings"
	"testing"
)

// lcXML builds a minimal lifecycle config XML with one Enabled
// Expiration.Days rule. prefix may be "" for a whole-bucket rule.
func lcXML(id, prefix string, days int) []byte {
	var b strings.Builder
	b.WriteString("<LifecycleConfiguration><Rule>")
	if id != "" {
		b.WriteString("<ID>" + id + "</ID>")
	}
	b.WriteString("<Status>Enabled</Status>")
	if prefix != "" {
		b.WriteString("<Filter><Prefix>" + prefix + "</Prefix></Filter>")
	}
	b.WriteString("<Expiration><Days>")
	// days is small in tests; fmt-free int->string
	b.WriteString(itoa(days))
	b.WriteString("</Days></Expiration></Rule></LifecycleConfiguration>")
	return []byte(b.String())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestFastpathWarn_Lengthened(t *testing.T) {
	old := lcXML("r1", "logs/", 7)
	new := lcXML("r1", "logs/", 30)
	got := fastpathConfigChangeLeavesStampedObjects(old, new)
	if got == "" {
		t.Fatalf("lengthen 7->30 must warn")
	}
	if !strings.Contains(got, "lengthened") || !strings.Contains(got, "7 -> 30") {
		t.Fatalf("unexpected reason: %q", got)
	}
}

func TestFastpathWarn_Shortened_NoWarn(t *testing.T) {
	old := lcXML("r1", "logs/", 30)
	new := lcXML("r1", "logs/", 7)
	if got := fastpathConfigChangeLeavesStampedObjects(old, new); got != "" {
		t.Fatalf("shorten 30->7 must not warn, got %q", got)
	}
}

func TestFastpathWarn_Removed(t *testing.T) {
	old := lcXML("r1", "logs/", 7)
	// new config has a different rule; r1 is gone
	new := lcXML("r2", "data/", 30)
	got := fastpathConfigChangeLeavesStampedObjects(old, new)
	if got == "" {
		t.Fatalf("removing an eligible rule must warn")
	}
	if !strings.Contains(got, "removed or disabled") || !strings.Contains(got, "r1") {
		t.Fatalf("unexpected reason: %q", got)
	}
}

func TestFastpathWarn_Delete(t *testing.T) {
	old := lcXML("r1", "logs/", 7)
	got := fastpathConfigChangeLeavesStampedObjects(old, nil)
	if got == "" {
		t.Fatalf("delete with an eligible rule must warn")
	}
	if !strings.Contains(got, "removed or disabled") {
		t.Fatalf("unexpected reason: %q", got)
	}
}

func TestFastpathWarn_Unchanged_NoWarn(t *testing.T) {
	old := lcXML("r1", "logs/", 7)
	new := lcXML("r1", "logs/", 7)
	if got := fastpathConfigChangeLeavesStampedObjects(old, new); got != "" {
		t.Fatalf("identical config must not warn, got %q", got)
	}
}

func TestFastpathWarn_NoOldRules_NoWarn(t *testing.T) {
	// fastpath off path: old had no eligible rules -> no warning even on delete
	if got := fastpathConfigChangeLeavesStampedObjects(nil, nil); got != "" {
		t.Fatalf("no old rules must not warn, got %q", got)
	}
	new := lcXML("r1", "logs/", 7)
	if got := fastpathConfigChangeLeavesStampedObjects(nil, new); got != "" {
		t.Fatalf("adding a rule (no old eligible) must not warn, got %q", got)
	}
}

func TestFastpathWarn_TagFilteredRule_NoWarn(t *testing.T) {
	// Tag-filtered rules are never on the fast path; removing one must not warn.
	old := []byte(`<LifecycleConfiguration><Rule><ID>rt</ID><Status>Enabled</Status>` +
		`<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>` +
		`<Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`)
	if got := fastpathConfigChangeLeavesStampedObjects(old, nil); got != "" {
		t.Fatalf("tag-only rule removal must not warn, got %q", got)
	}
}

func TestFastpathWarn_DisabledInNew_Warns(t *testing.T) {
	old := lcXML("r1", "logs/", 7)
	new := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Disabled</Status>` +
		`<Filter><Prefix>logs/</Prefix></Filter>` +
		`<Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`)
	got := fastpathConfigChangeLeavesStampedObjects(old, new)
	if got == "" {
		t.Fatalf("disabling an eligible rule must warn")
	}
}

func TestFastpathWarn_PrefixChanged_Warns(t *testing.T) {
	// Same ID, narrower prefix: old objects under "logs/" keep the old TTL
	// but the new rule only covers "logs/2026/". Treated as removed for the
	// old coverage -> warn.
	old := lcXML("r1", "logs/", 7)
	new := lcXML("r1", "logs/2026/", 7)
	got := fastpathConfigChangeLeavesStampedObjects(old, new)
	if got == "" {
		t.Fatalf("prefix change must warn")
	}
}

func TestFastpathWarn_OverflowDays_NoWarn(t *testing.T) {
	// ~68 years overflows int32 seconds; such a rule is never on the fast
	// path, so changing it must not warn.
	big := []byte(`<LifecycleConfiguration><Rule><ID>rb</ID><Status>Enabled</Status>` +
		`<Filter><Prefix>logs/</Prefix></Filter>` +
		`<Expiration><Days>30000</Days></Expiration></Rule></LifecycleConfiguration>`)
	if got := fastpathConfigChangeLeavesStampedObjects(big, nil); got != "" {
		t.Fatalf("overflow rule must not warn, got %q", got)
	}
}

func TestFastpathWarn_IDOnlyRename_NoWarn(t *testing.T) {
	// Renaming a rule while keeping the same prefix, size filters, and
	// days must not warn: the policy is unchanged, only the label moved.
	old := lcXML("r1", "logs/", 7)
	new := lcXML("r2", "logs/", 7)
	if got := fastpathConfigChangeLeavesStampedObjects(old, new); got != "" {
		t.Fatalf("ID-only rename must not warn, got %q", got)
	}
}

func TestFastpathWarn_IDRenameAndLengthen_Warns(t *testing.T) {
	// Renaming AND lengthening: the predicate match finds the successor
	// and the days increase triggers a lengthened warning.
	old := lcXML("r1", "logs/", 7)
	new := lcXML("r2", "logs/", 30)
	got := fastpathConfigChangeLeavesStampedObjects(old, new)
	if got == "" {
		t.Fatalf("rename + lengthen must warn")
	}
	if !strings.Contains(got, "lengthened") || !strings.Contains(got, "7 -> 30") {
		t.Fatalf("unexpected reason: %q", got)
	}
}

func TestFastpathWarn_IDRenameAndShorten_NoWarn(t *testing.T) {
	// Renaming AND shortening: not the data-loss direction.
	old := lcXML("r1", "logs/", 30)
	new := lcXML("r2", "logs/", 7)
	if got := fastpathConfigChangeLeavesStampedObjects(old, new); got != "" {
		t.Fatalf("rename + shorten must not warn, got %q", got)
	}
}
