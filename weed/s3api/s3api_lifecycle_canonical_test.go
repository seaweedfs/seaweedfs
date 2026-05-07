package s3api

import (
	"encoding/xml"
	"reflect"
	"testing"
	"time"
)

// parseLifecycle is a thin helper for tests; production code reads XML via the
// regular bucket-config decoder, this shortcut keeps the test focused on the
// canonical conversion.
func parseLifecycle(t *testing.T, xmlSrc string) *Lifecycle {
	t.Helper()
	lc := &Lifecycle{}
	if err := xml.Unmarshal([]byte(xmlSrc), lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return lc
}

func TestLifecycleToCanonical_TopLevelPrefix(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <ID>r1</ID>
    <Status>Enabled</Status>
    <Prefix>logs/</Prefix>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)
	if len(got) != 1 {
		t.Fatalf("want 1 rule, got %d", len(got))
	}
	r := got[0]
	if r.ID != "r1" || r.Status != "Enabled" || r.Prefix != "logs/" || r.ExpirationDays != 30 {
		t.Fatalf("unexpected: %+v", r)
	}
}

func TestLifecycleToCanonical_FilterPrefix(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)
	if got[0].Prefix != "logs/" {
		t.Fatalf("want logs/, got %q", got[0].Prefix)
	}
}

func TestLifecycleToCanonical_FilterTagAndSize(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>data/</Prefix>
        <Tag><Key>env</Key><Value>prod</Value></Tag>
        <Tag><Key>tier</Key><Value>cold</Value></Tag>
        <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
        <ObjectSizeLessThan>10485760</ObjectSizeLessThan>
      </And>
    </Filter>
    <Expiration><Days>90</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	if got.Prefix != "data/" {
		t.Fatalf("prefix want data/, got %q", got.Prefix)
	}
	wantTags := map[string]string{"env": "prod", "tier": "cold"}
	if !reflect.DeepEqual(got.FilterTags, wantTags) {
		t.Fatalf("tags want %v, got %v", wantTags, got.FilterTags)
	}
	if got.FilterSizeGreaterThan != 1024 || got.FilterSizeLessThan != 10485760 {
		t.Fatalf("size: gt=%d lt=%d", got.FilterSizeGreaterThan, got.FilterSizeLessThan)
	}
}

func TestLifecycleToCanonical_SingleTagFilter(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter>
      <Tag><Key>env</Key><Value>prod</Value></Tag>
    </Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	if !reflect.DeepEqual(got.FilterTags, map[string]string{"env": "prod"}) {
		t.Fatalf("tags want {env:prod}, got %v", got.FilterTags)
	}
}

func TestLifecycleToCanonical_MultipleActions(t *testing.T) {
	// One XML <Rule> with three concurrent actions: Expiration.Days,
	// NoncurrentVersionExpiration, AbortMultipartUpload. All must populate
	// the corresponding canonical fields so RuleActionKinds expands them
	// into three compiled actions downstream.
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <ID>multi</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>data/</Prefix></Filter>
    <Expiration><Days>90</Days></Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>30</NoncurrentDays>
      <NewerNoncurrentVersions>3</NewerNoncurrentVersions>
    </NoncurrentVersionExpiration>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	if got.ExpirationDays != 90 {
		t.Fatalf("ExpirationDays want 90, got %d", got.ExpirationDays)
	}
	if got.NoncurrentVersionExpirationDays != 30 || got.NewerNoncurrentVersions != 3 {
		t.Fatalf("NoncurrentVersion: %+v", got)
	}
	if got.AbortMPUDaysAfterInitiation != 7 {
		t.Fatalf("AbortMPU want 7, got %d", got.AbortMPUDaysAfterInitiation)
	}
}

func TestLifecycleToCanonical_ExpirationDate(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Date>2025-06-15T00:00:00Z</Date></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	want, _ := time.Parse(time.RFC3339, "2025-06-15T00:00:00Z")
	if !got.ExpirationDate.Equal(want) {
		t.Fatalf("date want %v, got %v", want, got.ExpirationDate)
	}
}

func TestLifecycleToCanonical_ExpiredObjectDeleteMarker(t *testing.T) {
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	if !got.ExpiredObjectDeleteMarker {
		t.Fatalf("want ExpiredObjectDeleteMarker=true")
	}
}

func TestLifecycleToCanonical_DisabledRulePreserved(t *testing.T) {
	// The engine's mode gate decides scheduling; conversion must preserve
	// the Status verbatim regardless.
	lc := parseLifecycle(t, `
<LifecycleConfiguration>
  <Rule>
    <Status>Disabled</Status>
    <Filter><Prefix>x/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)
	got := LifecycleToCanonical(lc)[0]
	if got.Status != "Disabled" {
		t.Fatalf("status want Disabled, got %q", got.Status)
	}
}

func TestLifecycleToCanonical_NilSafe(t *testing.T) {
	if got := LifecycleToCanonical(nil); got != nil {
		t.Fatalf("nil lc should return nil, got %v", got)
	}
}

func TestLifecycleToCanonical_EmptyRules(t *testing.T) {
	got := LifecycleToCanonical(&Lifecycle{})
	if len(got) != 0 {
		t.Fatalf("empty rules should be empty slice, got %d", len(got))
	}
}
