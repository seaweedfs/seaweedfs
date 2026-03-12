package blockvol

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageProfile_String(t *testing.T) {
	tests := []struct {
		p    StorageProfile
		want string
	}{
		{ProfileSingle, "single"},
		{ProfileStriped, "striped"},
		{StorageProfile(99), "unknown(99)"},
	}
	for _, tt := range tests {
		got := tt.p.String()
		if got != tt.want {
			t.Errorf("StorageProfile(%d).String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}

func TestParseStorageProfile_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  StorageProfile
	}{
		{"single", ProfileSingle},
		{"striped", ProfileStriped},
		{"", ProfileSingle},       // empty = backward compat
		{"Single", ProfileSingle},   // case-insensitive
		{"STRIPED", ProfileStriped},
		{" single ", ProfileSingle}, // whitespace-tolerant
	}
	for _, tt := range tests {
		got, err := ParseStorageProfile(tt.input)
		if err != nil {
			t.Errorf("ParseStorageProfile(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseStorageProfile(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseStorageProfile_Invalid(t *testing.T) {
	_, err := ParseStorageProfile("mirrored")
	if !errors.Is(err, ErrInvalidStorageProfile) {
		t.Errorf("ParseStorageProfile(mirrored) error = %v, want ErrInvalidStorageProfile", err)
	}
}

func TestStorageProfile_StringRoundTrip(t *testing.T) {
	for _, p := range []StorageProfile{ProfileSingle, ProfileStriped} {
		s := p.String()
		got, err := ParseStorageProfile(s)
		if err != nil {
			t.Errorf("round-trip %v -> %q: parse error: %v", p, s, err)
		}
		if got != p {
			t.Errorf("round-trip %v -> %q -> %v", p, s, got)
		}
	}
}

func TestSuperblock_ProfilePersistence(t *testing.T) {
	// ProfileSingle persists and reads back correctly.
	sb, err := NewSuperblock(1*1024*1024*1024, CreateOptions{
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("NewSuperblock: %v", err)
	}
	if sb.StorageProfile != uint8(ProfileSingle) {
		t.Fatalf("StorageProfile = %d, want %d", sb.StorageProfile, ProfileSingle)
	}

	var buf bytes.Buffer
	sb.WriteTo(&buf)

	got, err := ReadSuperblock(&buf)
	if err != nil {
		t.Fatalf("ReadSuperblock: %v", err)
	}
	if got.StorageProfile != uint8(ProfileSingle) {
		t.Errorf("StorageProfile = %d, want %d", got.StorageProfile, ProfileSingle)
	}
}

func TestSuperblock_BackwardCompat_ProfileZero(t *testing.T) {
	// Existing volumes have 0 at profile offset → ProfileSingle.
	sb, _ := NewSuperblock(1*1024*1024*1024, CreateOptions{})
	var buf bytes.Buffer
	sb.WriteTo(&buf)

	got, err := ReadSuperblock(&buf)
	if err != nil {
		t.Fatalf("ReadSuperblock: %v", err)
	}
	if StorageProfile(got.StorageProfile) != ProfileSingle {
		t.Errorf("backward compat: StorageProfile = %d, want %d (single)", got.StorageProfile, ProfileSingle)
	}
}

func TestSuperblock_InvalidProfileRejected(t *testing.T) {
	sb, _ := NewSuperblock(1*1024*1024*1024, CreateOptions{})
	sb.StorageProfile = 99

	err := sb.Validate()
	if err == nil {
		t.Error("Validate should reject StorageProfile=99")
	}
	if !errors.Is(err, ErrInvalidSuperblock) {
		t.Errorf("Validate error = %v, want ErrInvalidSuperblock", err)
	}
}

func TestCreate_WithProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()
	if vol.Profile() != ProfileSingle {
		t.Errorf("Profile() = %v, want single", vol.Profile())
	}
}

func TestCreate_DefaultProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()
	if vol.Profile() != ProfileSingle {
		t.Errorf("default Profile() = %v, want single", vol.Profile())
	}
}

func TestOpen_ProfileSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	vol.Close()

	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer vol2.Close()
	if vol2.Profile() != ProfileSingle {
		t.Errorf("Profile() after reopen = %v, want single", vol2.Profile())
	}
}

func TestCreate_StripedRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	_, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		StorageProfile: ProfileStriped,
	})
	if !errors.Is(err, ErrStripedNotImplemented) {
		t.Errorf("CreateBlockVol(striped) error = %v, want ErrStripedNotImplemented", err)
	}
	// File should not exist.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Error("CreateBlockVol(striped) should not leave a file behind")
	}
}

func TestCreate_InvalidProfileRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	_, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		StorageProfile: StorageProfile(99),
	})
	if !errors.Is(err, ErrInvalidStorageProfile) {
		t.Errorf("CreateBlockVol(profile=99) error = %v, want ErrInvalidStorageProfile", err)
	}
	// File should not exist.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Error("CreateBlockVol(invalid profile) should not leave a file behind")
	}
}

func TestOpen_InvalidProfileOnDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	vol.Close()

	// Corrupt the StorageProfile byte on disk (offset 105).
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt([]byte{99}, 105); err != nil {
		t.Fatalf("write corrupt byte: %v", err)
	}
	f.Close()

	_, err = OpenBlockVol(path)
	if err == nil {
		t.Fatal("OpenBlockVol should fail with invalid StorageProfile")
	}
}
