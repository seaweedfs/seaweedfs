package s3api

import "testing"

func TestBuildVersionedRemoteObjectPathRejectsTraversal(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}

	tests := []struct {
		name      string
		versionId string
		wantDir   string
		wantName  string
	}{
		{"valid version", "opaque_123", "/buckets/mybkt/obj.versions", "v_opaque_123"},
		{"traversal drops to unversioned", "v1/../../../buckets/victim/pwn", "/buckets/mybkt", "obj"},
		{"backslash drops to unversioned", `v1\..\victim`, "/buckets/mybkt", "obj"},
		{"empty is unversioned", "", "/buckets/mybkt", "obj"},
		{"null is unversioned", "null", "/buckets/mybkt", "obj"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, name := s3a.buildVersionedRemoteObjectPath("mybkt", "obj", tt.versionId)
			if dir != tt.wantDir || name != tt.wantName {
				t.Errorf("buildVersionedRemoteObjectPath(mybkt, obj, %q) = (%q, %q), want (%q, %q)",
					tt.versionId, dir, name, tt.wantDir, tt.wantName)
			}
		})
	}
}
