package s3tables

import (
	"strings"
	"testing"
)

func TestValidateNamespaceSupportsMultiLevel(t *testing.T) {
	got, err := validateNamespace([]string{"analytics", "daily"})
	if err != nil {
		t.Fatalf("validateNamespace returned error: %v", err)
	}
	if got != "analytics.daily" {
		t.Fatalf("validateNamespace = %q, want %q", got, "analytics.daily")
	}
}

func TestValidateNamespaceSupportsDottedInput(t *testing.T) {
	got, err := validateNamespace([]string{"analytics.daily"})
	if err != nil {
		t.Fatalf("validateNamespace returned error: %v", err)
	}
	if got != "analytics.daily" {
		t.Fatalf("validateNamespace = %q, want %q", got, "analytics.daily")
	}
}

func TestValidateNamespaceRejectsEmptyDottedSegment(t *testing.T) {
	_, err := validateNamespace([]string{"analytics..daily"})
	if err == nil {
		t.Fatalf("expected validateNamespace to fail for empty dotted segment")
	}
}

func TestParseNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      []string
		wantErr   bool
	}{
		{
			name:      "single level",
			namespace: "analytics",
			want:      []string{"analytics"},
		},
		{
			name:      "multi level dotted",
			namespace: "analytics.daily",
			want:      []string{"analytics", "daily"},
		},
		{
			name:      "invalid reserved prefix",
			namespace: "analytics.awsprod",
			wantErr:   true,
		},
		{
			name:      "invalid empty segment",
			namespace: "analytics..daily",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseNamespace(tt.namespace)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseNamespace(%q) expected error", tt.namespace)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseNamespace(%q) unexpected error: %v", tt.namespace, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ParseNamespace(%q) = %v, want %v", tt.namespace, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("ParseNamespace(%q) = %v, want %v", tt.namespace, got, tt.want)
				}
			}
		})
	}
}

func TestParseTableFromARNWithMultiLevelNamespace(t *testing.T) {
	arn := "arn:aws:s3tables:us-east-1:123456789012:bucket/testbucket/table/analytics.daily/events"
	bucket, namespace, table, err := parseTableFromARN(arn)
	if err != nil {
		t.Fatalf("parseTableFromARN returned error: %v", err)
	}
	if bucket != "testbucket" {
		t.Fatalf("bucket = %q, want %q", bucket, "testbucket")
	}
	if namespace != "analytics.daily" {
		t.Fatalf("namespace = %q, want %q", namespace, "analytics.daily")
	}
	if table != "events" {
		t.Fatalf("table = %q, want %q", table, "events")
	}
}

func TestBuildTableARNWithDottedNamespace(t *testing.T) {
	arn, err := BuildTableARN("us-east-1", "123456789012", "testbucket", "analytics.daily", "events")
	if err != nil {
		t.Fatalf("BuildTableARN returned error: %v", err)
	}
	if !strings.Contains(arn, "/table/analytics.daily/events") {
		t.Fatalf("BuildTableARN returned %q, missing normalized namespace/table path", arn)
	}
}

func TestExpandNamespace(t *testing.T) {
	got := expandNamespace("analytics.daily")
	want := []string{"analytics", "daily"}
	if len(got) != len(want) {
		t.Fatalf("expandNamespace = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("expandNamespace = %v, want %v", got, want)
		}
	}
}
