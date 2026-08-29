package winfsp

import "testing"

func TestVolumePrefix(t *testing.T) {
	tests := []struct {
		name       string
		mountPoint string
		prefix     string
		wantErr    bool
	}{
		{name: "drive letter", mountPoint: `S:`, prefix: ""},
		{name: "directory", mountPoint: `C:\mnt\weed`, prefix: ""},
		{name: "relative directory", mountPoint: `mnt\weed`, prefix: ""},
		{name: "unc", mountPoint: `\\seaweedfs\share`, prefix: `\seaweedfs\share`},
		{name: "unc trailing separator", mountPoint: `\\seaweedfs\share\`, prefix: `\seaweedfs\share`},
		{name: "unc forward slashes", mountPoint: `//seaweedfs/share`, prefix: `\seaweedfs\share`},
		{name: "unc dotted server", mountPoint: `\\fs.example.com\share`, prefix: `\fs.example.com\share`},
		{name: "mountmgr drive", mountPoint: `\\.\S:`, prefix: ""},
		{name: "extended drive", mountPoint: `\\?\S:`, prefix: ""},
		{name: "server only", mountPoint: `\\seaweedfs`, wantErr: true},
		{name: "too deep", mountPoint: `\\seaweedfs\share\dir`, wantErr: true},
		{name: "empty server", mountPoint: `\\\share`, wantErr: true},
		{name: "empty share", mountPoint: `\\seaweedfs\\`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, err := VolumePrefix(tt.mountPoint)
			if (err != nil) != tt.wantErr {
				t.Fatalf("VolumePrefix(%q) error = %v, wantErr %v", tt.mountPoint, err, tt.wantErr)
			}
			if prefix != tt.prefix {
				t.Fatalf("VolumePrefix(%q) = %q, want %q", tt.mountPoint, prefix, tt.prefix)
			}
		})
	}
}
