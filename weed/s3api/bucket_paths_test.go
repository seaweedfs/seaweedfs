package s3api

import "testing"

func TestBucketDirPreservesAbsoluteFilerPath(t *testing.T) {
	tests := []struct {
		name        string
		bucketsPath string
		bucket      string
		want        string
	}{
		{name: "empty root", bucketsPath: "", bucket: "bucket-a", want: "/bucket-a"},
		{name: "filesystem root", bucketsPath: "/", bucket: "bucket-a", want: "/bucket-a"},
		{name: "default root", bucketsPath: "/buckets", bucket: "bucket-a", want: "/buckets/bucket-a"},
		{name: "repeated separators", bucketsPath: "//buckets//", bucket: "bucket-a", want: "/buckets/bucket-a"},
		{name: "dot components", bucketsPath: "/data/../buckets/.", bucket: "bucket-a", want: "/buckets/bucket-a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: tt.bucketsPath}}
			if got := s3a.bucketDir(tt.bucket); got != tt.want {
				t.Fatalf("bucketDir(%q) = %q, want %q", tt.bucket, got, tt.want)
			}
			if got, want := s3a.toFilerPath(tt.bucket, "tenant/object.jpg"), tt.want+"/tenant/object.jpg"; got != want {
				t.Fatalf("toFilerPath() = %q, want %q", got, want)
			}
		})
	}
}
