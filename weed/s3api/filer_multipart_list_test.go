package s3api

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestSelectMultipartUploadsHoldsOnlyThePage(t *testing.T) {
	// a folder far larger than the page must not accumulate in the collector:
	// peak memory tracks maxUploads, not the open-upload count
	page := newMultipartUploadPage(listUploadsInput("", "", "", 3))
	const total = 20000
	for i := 0; i < total; i++ {
		page.add(uploadEntry(fmt.Sprintf("%05d", i), fmt.Sprintf("k%05d", total-1-i), int64(i)))
	}
	if page.top.Len() != 3 {
		t.Fatalf("collector holds %d candidates, want the 3-entry page", page.top.Len())
	}
	out := page.result()
	var got []string
	for _, u := range out.Upload {
		got = append(got, *u.Key+"/"+*u.UploadId)
	}
	want := []string{"k00000/19999", "k00001/19998", "k00002/19997"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if !*out.IsTruncated || *out.NextKeyMarker != "k00002" || *out.NextUploadIdMarker != "19997" {
		t.Fatalf("truncation markers got (%v, %v, %v)", *out.IsTruncated, *out.NextKeyMarker, *out.NextUploadIdMarker)
	}
}

// With encoding-type=url the markers and upload keys take the wire form the
// object listings already use: a client that echoes the escaped marker back
// still resumes at the decoded key.
func TestSelectMultipartUploadsUrlEncodesMarkersAndKeys(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "a\x01b", 10),
		uploadEntry("2", "a b", 20),
		uploadEntry("3", "b", 30),
	}
	input := listUploadsInput("", "", "", 1)
	input.EncodingType = aws.String(s3.EncodingTypeUrl)

	out := selectMultipartUploads(input, entries)
	if *out.Upload[0].Key != "a%01b" || *out.NextUploadIdMarker != "1" {
		t.Fatalf("page got key %q upload-id-marker %q", *out.Upload[0].Key, *out.NextUploadIdMarker)
	}
	marker, err := url.QueryUnescape(*out.NextKeyMarker)
	if err != nil || decodeListContinuationToken(marker) != "a\x01b" {
		t.Fatalf("next key marker %q does not resume at %q", *out.NextKeyMarker, "a\x01b")
	}

	input.KeyMarker = aws.String(decodeListContinuationToken(marker))
	input.UploadIdMarker = out.NextUploadIdMarker
	out = selectMultipartUploads(input, entries)
	if *out.KeyMarker != "a%01b" || *out.Upload[0].Key != "a+b" || *out.NextKeyMarker != "a+b" {
		t.Fatalf("page got marker %q key %q next %q", *out.KeyMarker, *out.Upload[0].Key, *out.NextKeyMarker)
	}

	input.KeyMarker = aws.String("a b")
	input.UploadIdMarker = out.NextUploadIdMarker
	out = selectMultipartUploads(input, entries)
	if *out.IsTruncated || *out.Upload[0].Key != "b" {
		t.Fatalf("last page got truncated=%v key %q", *out.IsTruncated, *out.Upload[0].Key)
	}
}

// A raw key that already carries the token prefix must survive an
// encoding-type=url round trip: under url mode the wire form is the url escape
// alone, so the client unescapes and hands the raw key back — running the
// token decode on "base64:..." would misread the raw key as an encoded token.
func TestSelectMultipartUploadsUrlTokenPrefixedKeyRoundTrip(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "base64:AQ", 10),
		uploadEntry("2", "base64:z", 20),
	}
	input := listUploadsInput("", "", "", 1)
	input.EncodingType = aws.String(s3.EncodingTypeUrl)

	out := selectMultipartUploads(input, entries)
	resume, err := url.QueryUnescape(*out.NextKeyMarker)
	if err != nil {
		t.Fatalf("next key marker %q does not unescape: %v", *out.NextKeyMarker, err)
	}
	if resume != "base64:AQ" {
		t.Fatalf("next key marker %q resumes at %q, want %q", *out.NextKeyMarker, resume, "base64:AQ")
	}

	input.KeyMarker = aws.String(resume)
	input.UploadIdMarker = out.NextUploadIdMarker
	out = selectMultipartUploads(input, entries)
	if *out.IsTruncated || len(out.Upload) != 1 || *out.Upload[0].Key != "base64%3Az" {
		t.Fatalf("second page got truncated=%v uploads %v", *out.IsTruncated, out.Upload)
	}
}

func TestSelectMultipartUploadsCapsRequestedPageSize(t *testing.T) {
	// max-uploads is caller-controlled; the page stays bounded at the server cap
	page := newMultipartUploadPage(listUploadsInput("", "", "", 10*maxUploadsList))
	if page.limit != maxUploadsList {
		t.Fatalf("page limit %d, want capped at %d", page.limit, maxUploadsList)
	}
	if got := *page.result().MaxUploads; got != int64(maxUploadsList) {
		t.Fatalf("MaxUploads reports %d, want the effective cap %d", got, maxUploadsList)
	}
}

// A transient failure mid-stream replays through a fresh page, so the uploads
// the aborted attempt already handed to the collector must not bleed into the
// result: each upload lists once and the markers track the retried attempt.
func TestListMultipartUploadsRetryCollectsEachUploadOnce(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "c", 10),
		uploadEntry("2", "a", 30),
		uploadEntry("3", "b", 10),
	}
	client := &listRetryClient{
		entries:   entries,
		recvErrs:  []error{status.Error(codes.Unavailable, "transport is closing"), nil},
		recvAfter: []int{2, 0},
	}
	accessor := &listRetryAccessor{client: client}
	input := listUploadsInput("", "", "", 2)

	var page *multipartUploadPage
	err := listRetry("/buckets/b/.uploads", func() error {
		page = newMultipartUploadPage(input)
		return filer_pb.List(context.Background(), accessor, "/buckets/b/.uploads", "", func(entry *filer_pb.Entry, isLastEntry bool) error {
			page.add(entry)
			return nil
		}, "", false, math.MaxInt32)
	})
	if err != nil {
		t.Fatalf("listRetry: %v", err)
	}
	if client.attempts != 2 {
		t.Fatalf("got %d attempts, want 2", client.attempts)
	}
	out := page.result()
	var got []string
	for _, u := range out.Upload {
		got = append(got, *u.Key+"/"+*u.UploadId)
	}
	want := []string{"a/2", "b/3"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v (no entry from the aborted attempt may carry over)", got, want)
	}
	if !*out.IsTruncated || *out.NextKeyMarker != "b" || *out.NextUploadIdMarker != "3" {
		t.Fatalf("truncation markers got (%v, %v, %v)", *out.IsTruncated, *out.NextKeyMarker, *out.NextUploadIdMarker)
	}
}

// With a delimiter, uploads whose keys go on past it after the prefix are
// listed once under CommonPrefixes, which count toward max-uploads.
func TestSelectMultipartUploadsDelimiter(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "a/1", 10),
		uploadEntry("2", "a/2", 10),
		uploadEntry("3", "b/x/1", 10),
		uploadEntry("4", "c", 10),
		uploadEntry("5", "a/sub/3", 10),
	}
	list := func(prefix, keyMarker string, maxUploads int64) ([]string, []string, bool, string) {
		input := listUploadsInput(prefix, keyMarker, "", maxUploads)
		input.Delimiter = aws.String("/")
		out := selectMultipartUploads(input, entries)
		var uploads, prefixes []string
		for _, u := range out.Upload {
			uploads = append(uploads, *u.Key)
		}
		for _, p := range out.CommonPrefixes {
			prefixes = append(prefixes, p.Prefix)
		}
		return uploads, prefixes, *out.IsTruncated, aws.StringValue(out.NextKeyMarker)
	}

	uploads, prefixes, truncated, _ := list("", "", 1000)
	if !slices.Equal(uploads, []string{"c"}) || !slices.Equal(prefixes, []string{"a/", "b/"}) || truncated {
		t.Errorf("no prefix: uploads %v prefixes %v truncated %v", uploads, prefixes, truncated)
	}

	uploads, prefixes, _, _ = list("a/", "", 1000)
	if !slices.Equal(uploads, []string{"a/1", "a/2"}) || !slices.Equal(prefixes, []string{"a/sub/"}) {
		t.Errorf("prefix a/: uploads %v prefixes %v", uploads, prefixes)
	}

	// a page that ends on a common prefix resumes after it
	uploads, prefixes, truncated, next := list("", "", 1)
	if len(uploads) != 0 || !slices.Equal(prefixes, []string{"a/"}) || !truncated || next != "a/" {
		t.Fatalf("page 1: uploads %v prefixes %v truncated %v next %q", uploads, prefixes, truncated, next)
	}
	uploads, prefixes, truncated, _ = list("", next, 1000)
	if !slices.Equal(uploads, []string{"c"}) || !slices.Equal(prefixes, []string{"b/"}) || truncated {
		t.Errorf("page 2: uploads %v prefixes %v truncated %v", uploads, prefixes, truncated)
	}
}

// A common prefix that does not fit the page is not retained: prefixes tracks
// only groups still in the page, so a small page stays bounded by max-uploads
// rather than by every prefix in the bucket.
func TestSelectMultipartUploadsDelimiterDropsLosingPrefixes(t *testing.T) {
	input := listUploadsInput("", "", "", 1)
	input.Delimiter = aws.String("/")
	page := newMultipartUploadPage(input)
	page.add(uploadEntry("1", "z/1", 10)) // holds the single slot, then evicted
	page.add(uploadEntry("2", "a/1", 10))
	page.add(uploadEntry("3", "z/2", 10)) // "z/" was evicted: loses again
	page.add(uploadEntry("4", "z/3", 10))
	if len(page.prefixes) > page.limit {
		t.Fatalf("retained %d prefixes with limit %d", len(page.prefixes), page.limit)
	}
	out := page.result()
	if len(out.CommonPrefixes) != 1 || out.CommonPrefixes[0].Prefix != "a/" || !*out.IsTruncated {
		t.Fatalf("prefixes %v truncated %v", out.CommonPrefixes, *out.IsTruncated)
	}
	if out.NextUploadIdMarker != nil {
		t.Fatalf("page ending on a common prefix has upload id marker %v", *out.NextUploadIdMarker)
	}
}
