package tests

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func render(t *testing.T, settings ...string) (string, []map[string]interface{}) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	args := []string{"template", "vector-test", ".."}
	for _, setting := range settings {
		args = append(args, "--set", setting)
	}
	output, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm: %v\n%s", err, output)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(output)))
	var documents []map[string]interface{}
	for {
		var document map[string]interface{}
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if document != nil {
			documents = append(documents, document)
		}
	}
	return string(output), documents
}

func TestVectorDeployment(t *testing.T) {
	_, defaults := render(t)
	for _, document := range defaults {
		if strings.HasSuffix(fmt.Sprint(document["metadata"].(map[string]interface{})["name"]), "-vector") {
			t.Fatal("vector resource enabled by default")
		}
	}
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("security-%v", secure), func(t *testing.T) {
			output, documents := render(t, "vector.enabled=true", "vector.metricsPort=9329", "vector.port=8098", "global.seaweedfs.enableSecurity="+fmt.Sprint(secure), "global.seaweedfs.monitoring.enabled=true", "networkPolicy.enabled=true")
			for _, want := range []string{"--listen=$(POD_IP):8098", "--metrics-port=9329", "path: /ready", "path: /health", "kind: ServiceMonitor"} {
				if !strings.Contains(output, want) {
					t.Fatalf("missing %s", want)
				}
			}
			found := false
			for _, document := range documents {
				metadata := document["metadata"].(map[string]interface{})
				if document["kind"] == "Deployment" && strings.HasSuffix(metadata["name"].(string), "-vector") {
					found = true
					spec := document["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
					if spec["nodeSelector"].(map[string]interface{})["kubernetes.io/os"] != "linux" {
						t.Fatal("vector engine must use Linux image platforms")
					}
				}
			}
			if !found {
				t.Fatal("no vector deployment")
			}
			if secure && (!strings.Contains(output, "[grpc.vector]") || !strings.Contains(output, "vector-cert")) {
				t.Fatal("missing vector TLS configuration")
			}
		})
	}
}

func TestVectorExistingImagesAndExternalCertificates(t *testing.T) {
	output, _ := render(t, "vector.enabled=true")
	start := strings.Index(output, "# Source: seaweedfs/templates/vector/vector-deployment.yaml")
	deployment := strings.Split(output[start:], "\n---")[0]
	if strings.Contains(deployment, "--metrics-port") || !strings.Contains(deployment, "tcpSocket:") {
		t.Fatal("default probes must support existing vector images")
	}
	output, _ = render(t, "vector.enabled=true", "global.seaweedfs.enableSecurity=true", "certificates.externalCertificates.enabled=true")
	if strings.Contains(output, "kind: Certificate") || !strings.Contains(output, "mountPath: /usr/local/share/ca-certificates/vector") {
		t.Fatal("external vector certificates are not mounted")
	}
}
