package dash

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestBuildEcVolumeRepairScriptWithCollection(t *testing.T) {
	got := buildEcVolumeRepairScript(123, "photos")
	want := "ec.rebuild -collection=photos -volumeIds=123 -apply"
	if got != want {
		t.Fatalf("repair script = %q, want %q", got, want)
	}
}

func TestBuildEcVolumeRepairScriptWithoutCollection(t *testing.T) {
	got := buildEcVolumeRepairScript(123, "")
	want := "ec.rebuild -volumeIds=123 -apply"
	if got != want {
		t.Fatalf("repair script = %q, want %q", got, want)
	}
}

func TestBuildEcVolumeRepairJobUsesAdminScript(t *testing.T) {
	job := buildEcVolumeRepairJob(123, "photos")
	if job.JobType != "admin_script" {
		t.Fatalf("job type = %q, want admin_script", job.JobType)
	}
	if job.Priority != plugin_pb.JobPriority_JOB_PRIORITY_HIGH {
		t.Fatalf("priority = %v, want high", job.Priority)
	}
	if !strings.HasPrefix(job.JobId, "ec-repair-123-") {
		t.Fatalf("job id = %q, want ec-repair-123-*", job.JobId)
	}
	if got := job.Labels["collection"]; got != "photos" {
		t.Fatalf("collection label = %q, want photos", got)
	}

	script, ok := job.Parameters["script"].Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok {
		t.Fatalf("script parameter has unexpected kind %T", job.Parameters["script"].Kind)
	}
	if script.StringValue != "ec.rebuild -collection=photos -volumeIds=123 -apply" {
		t.Fatalf("script = %q", script.StringValue)
	}
}

func TestValidateEcRepairCollectionRejectsCommandSeparators(t *testing.T) {
	for _, collection := range []string{"photos;volume.list", "photos other", "'photos'", "\"photos\"", "photos&rebuild", "photos|rebuild", "photos$rebuild"} {
		if err := ValidateEcRepairCollection(collection); err == nil {
			t.Fatalf("expected invalid collection %q to be rejected", collection)
		}
	}
}

func TestValidateEcRepairCollectionAcceptsSafeCharacters(t *testing.T) {
	for _, collection := range []string{"", "photos", "photos_2026", "photos-archive", "photos.archive"} {
		if err := ValidateEcRepairCollection(collection); err != nil {
			t.Fatalf("expected collection %q to be valid: %v", collection, err)
		}
	}
}
