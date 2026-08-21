package lifecycle_xml

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func TestLifecycleXMLRoundTrip_NoncurrentVersionExpiration(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>expire-noncurrent</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>30</NoncurrentDays>
      <NewerNoncurrentVersions>2</NewerNoncurrentVersions>
    </NoncurrentVersionExpiration>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(lc.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(lc.Rules))
	}
	rule := lc.Rules[0]
	if rule.ID != "expire-noncurrent" {
		t.Errorf("expected ID 'expire-noncurrent', got %q", rule.ID)
	}
	if rule.NoncurrentVersionExpiration.NoncurrentDays != 30 {
		t.Errorf("expected NoncurrentDays=30, got %d", rule.NoncurrentVersionExpiration.NoncurrentDays)
	}
	if rule.NoncurrentVersionExpiration.NewerNoncurrentVersions != 2 {
		t.Errorf("expected NewerNoncurrentVersions=2, got %d", rule.NoncurrentVersionExpiration.NewerNoncurrentVersions)
	}

	// Re-marshal and verify it round-trips.
	out, err := xml.Marshal(lc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "<NoncurrentDays>30</NoncurrentDays>") {
		t.Errorf("marshaled XML missing NoncurrentDays: %s", s)
	}
	if !strings.Contains(s, "<NewerNoncurrentVersions>2</NewerNoncurrentVersions>") {
		t.Errorf("marshaled XML missing NewerNoncurrentVersions: %s", s)
	}
}

func TestLifecycleXMLRoundTrip_AbortIncompleteMultipartUpload(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>abort-mpu</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rule := lc.Rules[0]
	if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation != 7 {
		t.Errorf("expected DaysAfterInitiation=7, got %d", rule.AbortIncompleteMultipartUpload.DaysAfterInitiation)
	}

	out, err := xml.Marshal(lc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), "<DaysAfterInitiation>7</DaysAfterInitiation>") {
		t.Errorf("marshaled XML missing DaysAfterInitiation: %s", string(out))
	}
}

func TestLifecycleXMLRoundTrip_FilterWithTag(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>tag-filter</ID>
    <Status>Enabled</Status>
    <Filter>
      <Tag><Key>env</Key><Value>dev</Value></Tag>
    </Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rule := lc.Rules[0]
	if !rule.Filter.TagSet() {
		t.Error("expected Filter.TagSet() to be true")
	}
	if rule.Filter.Tag.Key != "env" || rule.Filter.Tag.Value != "dev" {
		t.Errorf("expected Tag{env:dev}, got Tag{%s:%s}", rule.Filter.Tag.Key, rule.Filter.Tag.Value)
	}
}

func TestLifecycleXMLRoundTrip_FilterWithAnd(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>and-filter</ID>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>logs/</Prefix>
        <Tag><Key>env</Key><Value>dev</Value></Tag>
        <Tag><Key>tier</Key><Value>hot</Value></Tag>
        <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
        <ObjectSizeLessThan>1048576</ObjectSizeLessThan>
      </And>
    </Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rule := lc.Rules[0]
	if !rule.Filter.AndSet() {
		t.Error("expected Filter.AndSet() to be true")
	}
	if rule.Filter.And.Prefix.String() != "logs/" {
		t.Errorf("expected And.Prefix='logs/', got %q", rule.Filter.And.Prefix.String())
	}
	if len(rule.Filter.And.Tags) != 2 {
		t.Fatalf("expected 2 And tags, got %d", len(rule.Filter.And.Tags))
	}
	if rule.Filter.And.ObjectSizeGreaterThan != 1024 {
		t.Errorf("expected ObjectSizeGreaterThan=1024, got %d", rule.Filter.And.ObjectSizeGreaterThan)
	}
	if rule.Filter.And.ObjectSizeLessThan != 1048576 {
		t.Errorf("expected ObjectSizeLessThan=1048576, got %d", rule.Filter.And.ObjectSizeLessThan)
	}
}

func TestLifecycleXMLRoundTrip_FilterWithSizeOnly(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>size-filter</ID>
    <Status>Enabled</Status>
    <Filter>
      <ObjectSizeGreaterThan>512</ObjectSizeGreaterThan>
      <ObjectSizeLessThan>10485760</ObjectSizeLessThan>
    </Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rule := lc.Rules[0]
	if rule.Filter.ObjectSizeGreaterThan != 512 {
		t.Errorf("expected ObjectSizeGreaterThan=512, got %d", rule.Filter.ObjectSizeGreaterThan)
	}
	if rule.Filter.ObjectSizeLessThan != 10485760 {
		t.Errorf("expected ObjectSizeLessThan=10485760, got %d", rule.Filter.ObjectSizeLessThan)
	}
}

func TestLifecycleXML_TransitionSetFlag(t *testing.T) {
	// Verify that Transition.Set() is true after unmarshaling.
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>transition</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Transition>
      <Days>30</Days>
      <StorageClass>GLACIER</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !lc.Rules[0].Transition.Set() {
		t.Error("expected Transition.Set()=true after unmarshal")
	}
}

func TestLifecycleXML_NoncurrentVersionTransitionSetFlag(t *testing.T) {
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>nv-transition</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <NoncurrentVersionTransition>
      <NoncurrentDays>60</NoncurrentDays>
      <StorageClass>GLACIER</StorageClass>
    </NoncurrentVersionTransition>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !lc.Rules[0].NoncurrentVersionTransition.Set() {
		t.Error("expected NoncurrentVersionTransition.Set()=true after unmarshal")
	}
}

func TestLifecycleXMLRoundTrip_CompleteRule(t *testing.T) {
	// A complete lifecycle config similar to what Terraform sends.
	input := `<LifecycleConfiguration>
  <Rule>
    <ID>rotation</ID>
    <Filter><Prefix></Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>1</NoncurrentDays>
    </NoncurrentVersionExpiration>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>1</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>`

	var lc Lifecycle
	if err := xml.Unmarshal([]byte(input), &lc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rule := lc.Rules[0]
	if rule.ID != "rotation" {
		t.Errorf("expected ID 'rotation', got %q", rule.ID)
	}
	if rule.Expiration.Days != 30 {
		t.Errorf("expected Expiration.Days=30, got %d", rule.Expiration.Days)
	}
	if rule.NoncurrentVersionExpiration.NoncurrentDays != 1 {
		t.Errorf("expected NoncurrentDays=1, got %d", rule.NoncurrentVersionExpiration.NoncurrentDays)
	}
	if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation != 1 {
		t.Errorf("expected DaysAfterInitiation=1, got %d", rule.AbortIncompleteMultipartUpload.DaysAfterInitiation)
	}

	// Re-marshal and verify all fields survive.
	out, err := xml.Marshal(lc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	for _, expected := range []string{
		"<Days>30</Days>",
		"<NoncurrentDays>1</NoncurrentDays>",
		"<DaysAfterInitiation>1</DaysAfterInitiation>",
	} {
		if !strings.Contains(s, expected) {
			t.Errorf("marshaled XML missing %q: %s", expected, s)
		}
	}
}

// assertCanonicalRoundTrip drives a canonical rule through
// CanonicalToLifecycle -> MarshalCanonical -> ParseCanonical and checks the
// result matches the input, proving the admin write path (which only ever
// has the canonical form) produces XML the S3 API can read back unchanged.
func assertCanonicalRoundTrip(t *testing.T, in *s3lifecycle.Rule) *s3lifecycle.Rule {
	t.Helper()

	xmlBytes, err := MarshalCanonical([]*s3lifecycle.Rule{in})
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}

	out, err := ParseCanonical(xmlBytes)
	if err != nil {
		t.Fatalf("ParseCanonical(%s): %v", xmlBytes, err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 rule after round trip, got %d: %s", len(out), xmlBytes)
	}
	return out[0]
}

func TestCanonicalRoundTrip_WholeBucket(t *testing.T) {
	in := &s3lifecycle.Rule{ID: "whole-bucket", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30}
	out := assertCanonicalRoundTrip(t, in)
	if out.Prefix != "" {
		t.Errorf("expected empty prefix, got %q", out.Prefix)
	}
	if out.ExpirationDays != 30 {
		t.Errorf("expected ExpirationDays=30, got %d", out.ExpirationDays)
	}
}

func TestCanonicalRoundTrip_PrefixOnly(t *testing.T) {
	in := &s3lifecycle.Rule{ID: "prefix-only", Status: s3lifecycle.StatusEnabled, Prefix: "logs/", ExpirationDays: 7}
	out := assertCanonicalRoundTrip(t, in)
	if out.Prefix != "logs/" {
		t.Errorf("expected prefix 'logs/', got %q", out.Prefix)
	}
	if len(out.FilterTags) != 0 {
		t.Errorf("expected no tags, got %v", out.FilterTags)
	}
}

func TestCanonicalRoundTrip_TagOnly(t *testing.T) {
	in := &s3lifecycle.Rule{
		ID:             "tag-only",
		Status:         s3lifecycle.StatusEnabled,
		FilterTags:     map[string]string{"env": "dev"},
		ExpirationDays: 7,
	}
	out := assertCanonicalRoundTrip(t, in)
	if out.Prefix != "" {
		t.Errorf("expected empty prefix, got %q", out.Prefix)
	}
	if len(out.FilterTags) != 1 || out.FilterTags["env"] != "dev" {
		t.Errorf("expected tags {env:dev}, got %v", out.FilterTags)
	}
}

func TestCanonicalRoundTrip_PrefixAndTags(t *testing.T) {
	in := &s3lifecycle.Rule{
		ID:             "prefix-and-tags",
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         "logs/",
		FilterTags:     map[string]string{"env": "dev", "tier": "hot"},
		ExpirationDays: 7,
	}
	out := assertCanonicalRoundTrip(t, in)
	if out.Prefix != "logs/" {
		t.Errorf("expected prefix 'logs/', got %q", out.Prefix)
	}
	if len(out.FilterTags) != 2 || out.FilterTags["env"] != "dev" || out.FilterTags["tier"] != "hot" {
		t.Errorf("expected tags {env:dev, tier:hot}, got %v", out.FilterTags)
	}
}

func TestCanonicalRoundTrip_SizeBounds(t *testing.T) {
	in := &s3lifecycle.Rule{
		ID:                    "size-bounds",
		Status:                s3lifecycle.StatusEnabled,
		FilterSizeGreaterThan: 512,
		FilterSizeLessThan:    1048576,
		ExpirationDays:        30,
	}
	out := assertCanonicalRoundTrip(t, in)
	if out.FilterSizeGreaterThan != 512 {
		t.Errorf("expected FilterSizeGreaterThan=512, got %d", out.FilterSizeGreaterThan)
	}
	if out.FilterSizeLessThan != 1048576 {
		t.Errorf("expected FilterSizeLessThan=1048576, got %d", out.FilterSizeLessThan)
	}
}

func TestCanonicalRoundTrip_PrefixAndTagsWithSizeBounds(t *testing.T) {
	// Multiple discriminants (prefix + tags) force the <And> branch, which
	// carries its own size bounds distinct from the single-branch Filter.
	in := &s3lifecycle.Rule{
		ID:                    "and-with-size",
		Status:                s3lifecycle.StatusEnabled,
		Prefix:                "logs/",
		FilterTags:            map[string]string{"env": "dev"},
		FilterSizeGreaterThan: 1024,
		FilterSizeLessThan:    2048,
		ExpirationDays:        7,
	}
	out := assertCanonicalRoundTrip(t, in)
	if out.Prefix != "logs/" || len(out.FilterTags) != 1 {
		t.Errorf("expected prefix 'logs/' and 1 tag, got prefix=%q tags=%v", out.Prefix, out.FilterTags)
	}
	if out.FilterSizeGreaterThan != 1024 || out.FilterSizeLessThan != 2048 {
		t.Errorf("expected size bounds 1024/2048, got %d/%d", out.FilterSizeGreaterThan, out.FilterSizeLessThan)
	}
}

func TestCanonicalRoundTrip_ExpirationDate(t *testing.T) {
	date := time.Date(2030, 1, 15, 0, 0, 0, 0, time.UTC)
	in := &s3lifecycle.Rule{ID: "expire-on-date", Status: s3lifecycle.StatusEnabled, ExpirationDate: date}
	out := assertCanonicalRoundTrip(t, in)
	if !out.ExpirationDate.Equal(date) {
		t.Errorf("expected ExpirationDate=%v, got %v", date, out.ExpirationDate)
	}
}

func TestCanonicalRoundTrip_ExpiredObjectDeleteMarker(t *testing.T) {
	in := &s3lifecycle.Rule{ID: "delete-marker", Status: s3lifecycle.StatusEnabled, ExpiredObjectDeleteMarker: true}
	out := assertCanonicalRoundTrip(t, in)
	if !out.ExpiredObjectDeleteMarker {
		t.Error("expected ExpiredObjectDeleteMarker=true")
	}
}

func TestCanonicalRoundTrip_NoncurrentVersionExpiration(t *testing.T) {
	in := &s3lifecycle.Rule{
		ID:                              "noncurrent",
		Status:                          s3lifecycle.StatusEnabled,
		NoncurrentVersionExpirationDays: 30,
		NewerNoncurrentVersions:         2,
	}
	out := assertCanonicalRoundTrip(t, in)
	if out.NoncurrentVersionExpirationDays != 30 {
		t.Errorf("expected NoncurrentVersionExpirationDays=30, got %d", out.NoncurrentVersionExpirationDays)
	}
	if out.NewerNoncurrentVersions != 2 {
		t.Errorf("expected NewerNoncurrentVersions=2, got %d", out.NewerNoncurrentVersions)
	}
}

func TestCanonicalRoundTrip_AbortMultipartUpload(t *testing.T) {
	in := &s3lifecycle.Rule{ID: "abort-mpu", Status: s3lifecycle.StatusEnabled, AbortMPUDaysAfterInitiation: 7}
	out := assertCanonicalRoundTrip(t, in)
	if out.AbortMPUDaysAfterInitiation != 7 {
		t.Errorf("expected AbortMPUDaysAfterInitiation=7, got %d", out.AbortMPUDaysAfterInitiation)
	}
}

func TestCanonicalRoundTrip_Disabled(t *testing.T) {
	in := &s3lifecycle.Rule{ID: "disabled-rule", Status: s3lifecycle.StatusDisabled, ExpirationDays: 30}
	out := assertCanonicalRoundTrip(t, in)
	if out.Status != s3lifecycle.StatusDisabled {
		t.Errorf("expected Status=Disabled, got %q", out.Status)
	}
}

func TestCanonicalRoundTrip_TagOrderIsStable(t *testing.T) {
	// FilterTags is a map; And.Tags must be emitted in a deterministic
	// (sorted) order so re-saving an unchanged rule doesn't churn the
	// stored XML.
	in := &s3lifecycle.Rule{
		ID:         "stable-order",
		Status:     s3lifecycle.StatusEnabled,
		Prefix:     "logs/",
		FilterTags: map[string]string{"zeta": "1", "alpha": "2", "mu": "3"},
	}

	var firstXML []byte
	for i := 0; i < 5; i++ {
		xmlBytes, err := MarshalCanonical([]*s3lifecycle.Rule{in})
		if err != nil {
			t.Fatalf("MarshalCanonical: %v", err)
		}
		if i == 0 {
			firstXML = xmlBytes
			continue
		}
		if string(xmlBytes) != string(firstXML) {
			t.Fatalf("marshal output is not stable across runs:\n%s\nvs\n%s", firstXML, xmlBytes)
		}
	}
	if !strings.Contains(string(firstXML), "<Tag><Key>alpha</Key>") {
		t.Errorf("expected tags sorted alphabetically (alpha first), got: %s", firstXML)
	}
}
