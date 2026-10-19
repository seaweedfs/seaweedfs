package s3api

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// uploadEntry is an upload folder as createMultipartUpload leaves it.
func uploadEntry(id, key string, crtime int64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        id,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte(key)},
		Attributes:  &filer_pb.FuseAttributes{Crtime: crtime},
	}
}

func listUploadsInput(prefix, keyMarker, uploadIdMarker string, maxUploads int64) *s3.ListMultipartUploadsInput {
	return &s3.ListMultipartUploadsInput{
		Prefix:         aws.String(prefix),
		KeyMarker:      aws.String(keyMarker),
		UploadIdMarker: aws.String(uploadIdMarker),
		MaxUploads:     aws.Int64(maxUploads),
	}
}

func listedUploads(entries []*filer_pb.Entry, prefix, keyMarker, uploadIdMarker string, maxUploads int64) ([]string, bool) {
	out := selectMultipartUploads(listUploadsInput(prefix, keyMarker, uploadIdMarker, maxUploads), entries)
	var got []string
	for _, u := range out.Upload {
		got = append(got, *u.Key+"/"+*u.UploadId)
	}
	return got, *out.IsTruncated
}

func TestSelectMultipartUploads(t *testing.T) {
	// listed from the filer in upload id order, which is not key order
	entries := []*filer_pb.Entry{
		uploadEntry("1", "c", 10),
		uploadEntry("2", "a", 30),
		uploadEntry("3", "b", 10),
		uploadEntry("4", "a", 20),
		uploadEntry("5", "up/x", 10),
		// a folder with Extended set but no object key is not an upload
		{Name: "6", IsDirectory: true, Extended: map[string][]byte{}},
	}

	tests := []struct {
		name           string
		prefix         string
		keyMarker      string
		uploadIdMarker string
		maxUploads     int64
		want           []string
		truncated      bool
	}{
		{name: "sorted by key, then by upload id", maxUploads: 1000, want: []string{"a/2", "a/4", "b/3", "c/1", "up/x/5"}},
		{name: "key-marker starts after that key", keyMarker: "a", maxUploads: 1000, want: []string{"b/3", "c/1", "up/x/5"}},
		{name: "key-marker between keys", keyMarker: "bb", maxUploads: 1000, want: []string{"c/1", "up/x/5"}},
		{name: "upload-id-marker continues inside its key", keyMarker: "a", uploadIdMarker: "2", maxUploads: 1000, want: []string{"a/4", "b/3", "c/1", "up/x/5"}},
		{name: "upload-id-marker of an aborted upload", keyMarker: "a", uploadIdMarker: "3", maxUploads: 1000, want: []string{"a/4", "b/3", "c/1", "up/x/5"}},
		{name: "upload-id-marker without key-marker is ignored", uploadIdMarker: "4", maxUploads: 1000, want: []string{"a/2", "a/4", "b/3", "c/1", "up/x/5"}},
		{name: "prefix", prefix: "up/", maxUploads: 1000, want: []string{"up/x/5"}},
		{name: "page", maxUploads: 2, want: []string{"a/2", "a/4"}, truncated: true},
		{name: "exact last page is not truncated", keyMarker: "b", maxUploads: 2, want: []string{"c/1", "up/x/5"}},
		{name: "max-uploads 0", maxUploads: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, truncated := listedUploads(entries, tt.prefix, tt.keyMarker, tt.uploadIdMarker, tt.maxUploads)
			if !slices.Equal(got, tt.want) || truncated != tt.truncated {
				t.Fatalf("got %v truncated=%v, want %v truncated=%v", got, truncated, tt.want, tt.truncated)
			}
		})
	}
}

func TestSelectMultipartUploadsPaginatesEveryUploadOnce(t *testing.T) {
	var entries []*filer_pb.Entry
	for i, key := range []string{"k3", "k1", "k2", "k1", "k3", "k1", "k2"} {
		entries = append(entries, uploadEntry(string(rune('a'+i)), key, int64(i)))
	}
	seen := map[string]bool{}
	input := listUploadsInput("", "", "", 2)
	pages := 0
	for {
		out := selectMultipartUploads(input, entries)
		for _, u := range out.Upload {
			if seen[*u.UploadId] {
				t.Fatalf("upload %s listed twice", *u.UploadId)
			}
			seen[*u.UploadId] = true
		}
		pages++
		if !*out.IsTruncated {
			break
		}
		if out.NextKeyMarker == nil || out.NextUploadIdMarker == nil {
			t.Fatalf("truncated page without next markers")
		}
		input.KeyMarker, input.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	if pages != 4 {
		t.Fatalf("got %d pages, want 4", pages)
	}
	if len(seen) != len(entries) {
		t.Fatalf("listed %d of %d uploads", len(seen), len(entries))
	}
}
