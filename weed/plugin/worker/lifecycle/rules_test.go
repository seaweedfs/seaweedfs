package lifecycle

import (
	"testing"
	"time"
)

func TestParseLifecycleXML_CompleteConfig(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>rotation</ID>
    <Filter><Prefix></Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>7</NoncurrentDays>
      <NewerNoncurrentVersions>2</NewerNoncurrentVersions>
    </NoncurrentVersionExpiration>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>3</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}

	r := rules[0]
	if r.ID != "rotation" {
		t.Errorf("expected ID 'rotation', got %q", r.ID)
	}
	if r.Status != "Enabled" {
		t.Errorf("expected Status 'Enabled', got %q", r.Status)
	}
	if r.ExpirationDays != 30 {
		t.Errorf("expected ExpirationDays=30, got %d", r.ExpirationDays)
	}
	if r.NoncurrentVersionExpirationDays != 7 {
		t.Errorf("expected NoncurrentVersionExpirationDays=7, got %d", r.NoncurrentVersionExpirationDays)
	}
	if r.NewerNoncurrentVersions != 2 {
		t.Errorf("expected NewerNoncurrentVersions=2, got %d", r.NewerNoncurrentVersions)
	}
	if r.AbortMPUDaysAfterInitiation != 3 {
		t.Errorf("expected AbortMPUDaysAfterInitiation=3, got %d", r.AbortMPUDaysAfterInitiation)
	}
}

func TestParseLifecycleXML_PrefixFilter(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>logs</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Prefix != "logs/" {
		t.Errorf("expected Prefix='logs/', got %q", rules[0].Prefix)
	}
}

func TestParseLifecycleXML_LegacyPrefix(t *testing.T) {
	// Old-style <Prefix> at rule level instead of inside <Filter>.
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>old</ID>
    <Status>Enabled</Status>
    <Prefix>archive/</Prefix>
    <Expiration><Days>90</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Prefix != "archive/" {
		t.Errorf("expected Prefix='archive/', got %q", rules[0].Prefix)
	}
}

func TestParseLifecycleXML_TagFilter(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>tag-rule</ID>
    <Status>Enabled</Status>
    <Filter>
      <Tag><Key>env</Key><Value>dev</Value></Tag>
    </Filter>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	r := rules[0]
	if len(r.FilterTags) != 1 || r.FilterTags["env"] != "dev" {
		t.Errorf("expected FilterTags={env:dev}, got %v", r.FilterTags)
	}
}

func TestParseLifecycleXML_AndFilter(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>and-rule</ID>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>data/</Prefix>
        <Tag><Key>env</Key><Value>staging</Value></Tag>
        <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
      </And>
    </Filter>
    <Expiration><Days>14</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	r := rules[0]
	if r.Prefix != "data/" {
		t.Errorf("expected Prefix='data/', got %q", r.Prefix)
	}
	if r.FilterTags["env"] != "staging" {
		t.Errorf("expected tag env=staging, got %v", r.FilterTags)
	}
	if r.FilterSizeGreaterThan != 1024 {
		t.Errorf("expected FilterSizeGreaterThan=1024, got %d", r.FilterSizeGreaterThan)
	}
}

func TestParseLifecycleXML_ExpirationDate(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>date-rule</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Date>2026-06-01T00:00:00Z</Date></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	expected := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if !rules[0].ExpirationDate.Equal(expected) {
		t.Errorf("expected ExpirationDate=%v, got %v", expected, rules[0].ExpirationDate)
	}
}

func TestParseLifecycleXML_ExpiredObjectDeleteMarker(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>marker-cleanup</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if !rules[0].ExpiredObjectDeleteMarker {
		t.Error("expected ExpiredObjectDeleteMarker=true")
	}
}

func TestParseLifecycleXML_MultipleRules(t *testing.T) {
	xml := []byte(`<LifecycleConfiguration>
  <Rule>
    <ID>rule1</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
  <Rule>
    <ID>rule2</ID>
    <Status>Disabled</Status>
    <Filter><Prefix>temp/</Prefix></Filter>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
  <Rule>
    <ID>rule3</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>365</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`)

	rules, err := parseLifecycleXML(xml)
	if err != nil {
		t.Fatalf("parseLifecycleXML: %v", err)
	}
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}
	if rules[1].Status != "Disabled" {
		t.Errorf("expected rule2 Status=Disabled, got %q", rules[1].Status)
	}
}

func TestParseExpirationDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{"rfc3339_utc", "2026-06-01T00:00:00Z", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), false},
		{"rfc3339_offset", "2026-06-01T00:00:00+05:00", time.Date(2026, 6, 1, 0, 0, 0, 0, time.FixedZone("", 5*3600)), false},
		{"date_only", "2026-06-01", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), false},
		{"invalid", "not-a-date", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExpirationDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExpirationDate(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("parseExpirationDate(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
