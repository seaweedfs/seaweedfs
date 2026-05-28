package s3api

import (
	"net/http"
	"testing"
)

func TestResolveDestinationMime(t *testing.T) {
	cases := []struct {
		name        string
		reqCT       string
		srcMime     string
		replaceMeta bool
		want        string
	}{
		{"COPY keeps source mime", "text/plain", "image/png", false, "image/png"},
		{"COPY without request CT", "", "image/png", false, "image/png"},
		{"COPY with empty source mime", "text/plain", "", false, ""},
		{"REPLACE with request CT wins", "text/plain", "image/png", true, "text/plain"},
		{"REPLACE without request CT uses MinIO default", "", "image/png", true, defaultCopyContentType},
		{"REPLACE with request CT and empty source", "application/json", "", true, "application/json"},
		{"REPLACE without request CT and empty source", "", "", true, defaultCopyContentType},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := http.Header{}
			if c.reqCT != "" {
				h.Set("Content-Type", c.reqCT)
			}
			got := resolveDestinationMime(h, c.srcMime, c.replaceMeta)
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestIsValidDirective(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{"", true},
		{DirectiveCopy, true},
		{DirectiveReplace, true},
		{"copy", false},
		{"replace", false},
		{"FOO", false},
		{"REPLACE ", false},
	}
	for _, c := range cases {
		t.Run(c.value, func(t *testing.T) {
			if got := isValidDirective(c.value); got != c.want {
				t.Errorf("isValidDirective(%q) = %v, want %v", c.value, got, c.want)
			}
		})
	}
}

func TestProcessMetadataBytes_ReplaceSystemHeaders(t *testing.T) {
	existing := map[string][]byte{
		"Cache-Control":       []byte("max-age=60"),
		"Content-Disposition": []byte(`attachment; filename="old.bin"`),
	}
	req := http.Header{}
	req.Set("Cache-Control", "no-cache")
	req.Set("Content-Encoding", "gzip")

	out, err := processMetadataBytes(req, existing, true /* replaceMeta */, false /* replaceTagging */)
	if err != nil {
		t.Fatalf("processMetadataBytes returned error: %v", err)
	}
	if got := string(out["Cache-Control"]); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", got, "no-cache")
	}
	if got := string(out["Content-Encoding"]); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", got, "gzip")
	}
	if _, present := out["Content-Disposition"]; present {
		t.Errorf("Content-Disposition should be dropped under REPLACE when not in request, got %q", string(out["Content-Disposition"]))
	}
}

func TestIsManagedCopyMetadataKey_CoversSystemHeaders(t *testing.T) {
	for _, h := range copyReplaceSystemHeaders {
		if !isManagedCopyMetadataKey(h) {
			t.Errorf("isManagedCopyMetadataKey(%q) = false, want true so mergeCopyMetadata drops stale source values", h)
		}
	}
}

func TestIsManagedCopyMetadataKey_CaseInsensitive(t *testing.T) {
	cases := []string{
		"cache-control",
		"CACHE-CONTROL",
		"content-encoding",
		"x-amz-meta-owner",
		"X-AMZ-META-OWNER",
		"x-amz-tagging-env",
	}
	for _, k := range cases {
		t.Run(k, func(t *testing.T) {
			if !isManagedCopyMetadataKey(k) {
				t.Errorf("isManagedCopyMetadataKey(%q) = false, want true; legacy non-canonical keys must still be recognized so mergeCopyMetadata clears them", k)
			}
		})
	}
}

func TestProcessMetadataBytes_CopyAcceptsLowercaseSourceKeys(t *testing.T) {
	existing := map[string][]byte{
		"cache-control":       []byte("max-age=60"),
		"content-disposition": []byte(`attachment; filename="legacy.bin"`),
		"CONTENT-ENCODING":    []byte("gzip"),
		"Content-language":    []byte("en"),
	}
	req := http.Header{}

	out, err := processMetadataBytes(req, existing, false, false)
	if err != nil {
		t.Fatalf("processMetadataBytes error: %v", err)
	}
	if got := string(out["Cache-Control"]); got != "max-age=60" {
		t.Errorf("Cache-Control = %q, want %q (lowercase source must be promoted to canonical)", got, "max-age=60")
	}
	if got := string(out["Content-Disposition"]); got != `attachment; filename="legacy.bin"` {
		t.Errorf("Content-Disposition = %q, want legacy source value promoted to canonical", got)
	}
	if got := string(out["Content-Encoding"]); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q (uppercase source must be promoted to canonical)", got, "gzip")
	}
	if got := string(out["Content-Language"]); got != "en" {
		t.Errorf("Content-Language = %q, want %q (mixed-case source must be promoted to canonical)", got, "en")
	}
}

func TestProcessMetadataBytes_CopyAcceptsLowercaseTagKeys(t *testing.T) {
	existing := map[string][]byte{
		"x-amz-tagging-env":  []byte("prod"),
		"X-AMZ-TAGGING-team": []byte("infra"),
	}
	req := http.Header{}

	out, err := processMetadataBytes(req, existing, false /* replaceMeta */, false /* replaceTagging */)
	if err != nil {
		t.Fatalf("processMetadataBytes error: %v", err)
	}
	if got := string(out["X-Amz-Tagging-env"]); got != "prod" {
		t.Errorf("X-Amz-Tagging-env = %q, want %q (legacy lowercase tag must be promoted to canonical)", got, "prod")
	}
	if got := string(out["X-Amz-Tagging-team"]); got != "infra" {
		t.Errorf("X-Amz-Tagging-team = %q, want %q (uppercase tag must be promoted to canonical)", got, "infra")
	}
}

func TestMergeCopyMetadata_ReplaceDropsStaleSystemHeader(t *testing.T) {
	// End-to-end caller flow: source-populated Extended + REPLACE request
	// must drop stale managed values, keep non-managed ones.
	existing := map[string][]byte{
		"Content-Disposition":          []byte(`attachment; filename="old.bin"`),
		"Cache-Control":                []byte("max-age=60"),
		"X-Amz-Meta-Owner":             []byte("old"),
		"X-Amz-Meta-OnlyOnSource":      []byte("should-drop"),
		"X-Custom-Non-Managed":         []byte("keep-me"),
		"X-Amz-Server-Side-Encryption": []byte("aws:kms"),
	}
	req := http.Header{}
	req.Set("Cache-Control", "no-cache")
	req.Set("X-Amz-Meta-Owner", "new")

	processed, err := processMetadataBytes(req, existing, true, false)
	if err != nil {
		t.Fatalf("processMetadataBytes error: %v", err)
	}

	merged := mergeCopyMetadata(existing, processed)

	if _, leak := merged["Content-Disposition"]; leak {
		t.Errorf("Content-Disposition leaked through REPLACE merge: %q", string(merged["Content-Disposition"]))
	}
	if got := string(merged["Cache-Control"]); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", got, "no-cache")
	}
	if got := string(merged["X-Amz-Meta-Owner"]); got != "new" {
		t.Errorf("X-Amz-Meta-Owner = %q, want %q", got, "new")
	}
	if _, leak := merged["X-Amz-Meta-OnlyOnSource"]; leak {
		t.Errorf("stale X-Amz-Meta-OnlyOnSource must be dropped under REPLACE, still present: %q", string(merged["X-Amz-Meta-OnlyOnSource"]))
	}
	if got := string(merged["X-Custom-Non-Managed"]); got != "keep-me" {
		t.Errorf("non-managed key %q must survive REPLACE merge, got %q", "X-Custom-Non-Managed", got)
	}
}

func TestMergeCopyMetadata_ReplaceTaggingDropsStaleTags(t *testing.T) {
	// REPLACE tagging directive must drop old object tags that the new
	// request doesn't redeclare.
	existing := map[string][]byte{
		"X-Amz-Tagging-old": []byte("v1"),
		"X-Amz-Tagging-env": []byte("dev"),
		"X-Amz-Meta-Author": []byte("alice"),
	}
	req := http.Header{}
	req.Set("X-Amz-Tagging", "env=prod")

	processed, err := processMetadataBytes(req, existing, false /* replaceMeta */, true /* replaceTagging */)
	if err != nil {
		t.Fatalf("processMetadataBytes error: %v", err)
	}

	merged := mergeCopyMetadata(existing, processed)

	if _, leak := merged["X-Amz-Tagging-old"]; leak {
		t.Errorf("stale tag X-Amz-Tagging-old must be dropped, still present: %q", string(merged["X-Amz-Tagging-old"]))
	}
	if got := string(merged["X-Amz-Tagging-env"]); got != "prod" {
		t.Errorf("X-Amz-Tagging-env = %q, want %q", got, "prod")
	}
	if got := string(merged["X-Amz-Meta-Author"]); got != "alice" {
		t.Errorf("COPY-mode user metadata must survive tag-only REPLACE: got %q", got)
	}
}

func TestProcessMetadataBytes_CopyInheritsSystemHeaders(t *testing.T) {
	existing := map[string][]byte{
		"Cache-Control":       []byte("max-age=60"),
		"Content-Disposition": []byte(`attachment; filename="src.bin"`),
		"Content-Encoding":    []byte("gzip"),
	}
	req := http.Header{}
	req.Set("Cache-Control", "no-cache")

	out, err := processMetadataBytes(req, existing, false /* replaceMeta */, false /* replaceTagging */)
	if err != nil {
		t.Fatalf("processMetadataBytes returned error: %v", err)
	}
	if got := string(out["Cache-Control"]); got != "max-age=60" {
		t.Errorf("Cache-Control = %q, want %q (source value, request ignored under COPY)", got, "max-age=60")
	}
	if got := string(out["Content-Disposition"]); got != `attachment; filename="src.bin"` {
		t.Errorf("Content-Disposition = %q, want source value", got)
	}
	if got := string(out["Content-Encoding"]); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", got, "gzip")
	}
}
