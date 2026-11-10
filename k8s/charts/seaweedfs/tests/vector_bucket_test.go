package tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVectorBucketHook(t *testing.T) {
	for _, auth := range []bool{false, true} {
		t.Run(fmt.Sprint(auth), func(t *testing.T) {
			output, documents := render(t, "s3.enabled=true", "vector.enabled=true", "vector.createBuckets[0]=embeddings", "s3.enableAuth="+fmt.Sprint(auth), "networkPolicy.enabled=true")
			var found bool
			for _, document := range documents {
				metadata := document["metadata"].(map[string]interface{})
				if document["kind"] == "Job" && strings.HasSuffix(metadata["name"].(string), "-vector-bucket-hook") {
					found = true
					if metadata["annotations"].(map[string]interface{})["helm.sh/hook"] != "post-install,post-upgrade" {
						t.Fatal("missing upgrade hook")
					}
				}
			}
			if !found || !strings.Contains(output, "s3vectors get-vector-bucket") || !strings.Contains(output, "s3vectors create-vector-bucket") {
				t.Fatal("missing vector bucket provisioning")
			}
			if auth && !strings.Contains(output, "key: \"admin_access_key_id\"") {
				t.Fatal("missing generated admin credential reference")
			}
		})
	}
}

func TestVectorBucketHookRetriesRacesAndRefusesConflicts(t *testing.T) {
	_, documents := render(t, "s3.enabled=true", "vector.enabled=true", "vector.createBuckets[0]=embeddings")
	var script string
	for _, document := range documents {
		if document["kind"] != "Job" {
			continue
		}
		metadata := document["metadata"].(map[string]interface{})
		if strings.HasSuffix(metadata["name"].(string), "-vector-bucket-hook") {
			pod := document["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
			container := pod["containers"].([]interface{})[0].(map[string]interface{})
			script = container["args"].([]interface{})[0].(string)
		}
	}
	if script == "" {
		t.Fatal("missing hook script")
	}
	for _, mode := range []string{"missing", "existing", "race", "iceberg", "denied"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			stub := `#!/bin/sh
shift 2
operation=$2
case "$operation" in
get-vector-bucket)
  if [ "$BUCKET_MODE" = denied ]; then echo AccessDenied >&2; exit 1; fi
  if [ "$BUCKET_MODE" = existing ] || { [ -f "$BUCKET_STATE" ] && [ "$BUCKET_MODE" != iceberg ]; }; then exit 0; fi
  echo NotFoundException >&2; exit 1;;
create-vector-bucket)
  touch "$BUCKET_STATE"
  if [ "$BUCKET_MODE" = race ] || [ "$BUCKET_MODE" = iceberg ]; then exit 1; fi;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "aws"), []byte(stub), 0755); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("sh", "-ec", script)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "S3_ENDPOINT=http://unused", "BUCKET_MODE="+mode, "BUCKET_STATE="+filepath.Join(dir, "state"))
			output, err := command.CombinedOutput()
			wantFailure := mode == "iceberg" || mode == "denied"
			if (err != nil) != wantFailure {
				t.Fatalf("mode %s: %v %s", mode, err, output)
			}
		})
	}
}

func TestVectorBucketHookRejectsShellNames(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	command := exec.Command("helm", "template", "test", "..", "--set", "s3.enabled=true", "--set", "vector.enabled=true", "--set", "vector.createBuckets[0]=bucket;touch /tmp/invalid")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "vector.createBuckets names") {
		t.Fatalf("invalid bucket accepted: %v %s", err, output)
	}
}

func TestVectorBucketHookGatewayModes(t *testing.T) {
	for _, mode := range []string{"s3", "filer", "allInOne"} {
		settings := []string{"vector.enabled=true", "vector.createBuckets[0]=embeddings"}
		component := "s3"
		switch mode {
		case "s3":
			settings = append(settings, "s3.enabled=true", "s3.enableAuth=true")
		case "filer":
			settings = append(settings, "s3.enabled=false", "filer.s3.enabled=true", "filer.s3.enableAuth=true")
			component = "filer"
		case "allInOne":
			settings = append(settings, "s3.enabled=false", "allInOne.enabled=true", "allInOne.s3.enabled=true", "allInOne.s3.enableAuth=true")
			component = "all-in-one"
		}
		output, _ := render(t, settings...)
		if !strings.Contains(output, "http://vector-test-seaweedfs-"+component+".default:8333") || !strings.Contains(output, "key: \"admin_access_key_id\"") {
			t.Fatalf("missing endpoint or auth for %s", mode)
		}
	}
}
