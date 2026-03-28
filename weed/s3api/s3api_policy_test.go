package s3api

import (
	"encoding/xml"
	"strings"
	"testing"
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
	if !rule.Filter.tagSet {
		t.Error("expected Filter.tagSet to be true")
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
	if !rule.Filter.andSet {
		t.Error("expected Filter.andSet to be true")
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
