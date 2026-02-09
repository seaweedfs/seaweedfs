package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestLoadStorageClassDiskTypeMap(t *testing.T) {
	mappings := loadStorageClassDiskTypeMap(map[string]string{
		"STANDARD_IA": "nvme",
	})

	if got, want := mappings["STANDARD_IA"], "nvme"; got != want {
		t.Fatalf("STANDARD_IA mapping mismatch: got %q want %q", got, want)
	}
	if got, want := mappings["STANDARD"], "ssd"; got != want {
		t.Fatalf("STANDARD default mismatch: got %q want %q", got, want)
	}
	if got, want := mappings["GLACIER"], "hdd"; got != want {
		t.Fatalf("GLACIER default mismatch: got %q want %q", got, want)
	}
}

func TestResolveEffectiveStorageClass(t *testing.T) {
	header := make(http.Header)
	header.Set(s3_constants.AmzStorageClass, "standard_ia")
	sc, code := resolveEffectiveStorageClass(header, nil)
	if code != s3err.ErrNone {
		t.Fatalf("expected no error, got %v", code)
	}
	if sc != "STANDARD_IA" {
		t.Fatalf("expected STANDARD_IA, got %q", sc)
	}

	header = make(http.Header)
	sc, code = resolveEffectiveStorageClass(header, map[string][]byte{
		s3_constants.AmzStorageClass: []byte("GLACIER"),
	})
	if code != s3err.ErrNone {
		t.Fatalf("expected no error for entry metadata, got %v", code)
	}
	if sc != "GLACIER" {
		t.Fatalf("expected GLACIER, got %q", sc)
	}

	sc, code = resolveEffectiveStorageClass(header, nil)
	if code != s3err.ErrNone {
		t.Fatalf("expected no error for default class, got %v", code)
	}
	if sc != defaultStorageClass {
		t.Fatalf("expected default storage class %q, got %q", defaultStorageClass, sc)
	}
}

func TestResolveEffectiveStorageClassRejectsInvalidHeader(t *testing.T) {
	header := make(http.Header)
	header.Set(s3_constants.AmzStorageClass, "not-a-class")
	_, code := resolveEffectiveStorageClass(header, nil)
	if code != s3err.ErrInvalidStorageClass {
		t.Fatalf("expected ErrInvalidStorageClass, got %v", code)
	}
}
