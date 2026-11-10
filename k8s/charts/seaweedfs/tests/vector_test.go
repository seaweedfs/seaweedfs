package tests

import (
	"fmt"
	"io"
	"os"
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
			output, documents := render(t, "vector.enabled=true", "vector.metricsPort=9329", "vector.imageOverride=example/metrics-engine:test", "vector.port=8098", "global.seaweedfs.enableSecurity="+fmt.Sprint(secure), "global.seaweedfs.monitoring.enabled=true", "networkPolicy.enabled=true")
			for _, want := range []string{"--listen=\"$POD_IP:8098\"", "--metrics-port=9329", "path: /ready", "path: /health", "kind: ServiceMonitor"} {
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

func TestVectorIPv6Listen(t *testing.T) {
	_, docs := render(t, "vector.enabled=true")
	var args []interface{}
	for _, doc := range docs {
		if doc["kind"] != "Deployment" {
			continue
		}
		pod := doc["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
		c := pod["containers"].([]interface{})[0].(map[string]interface{})
		if c["name"] == "vector" {
			args = c["args"].([]interface{})
		}
	}
	for _, ip := range []string{"10.0.0.20", "fd00::20"} {
		dir := t.TempDir()
		script := strings.ReplaceAll(args[0].(string), "/usr/bin/weed-vector", "printf '%s\\n'")
		cmd := exec.Command("sh", "-ec", script, "vector", "--masters=master")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "POD_IP="+ip)
		out, err := cmd.CombinedOutput()
		address := ip
		if strings.Contains(ip, ":") {
			address = "[" + ip + "]"
		}
		if err != nil || !strings.Contains(string(out), "--listen="+address+":8090") || !strings.Contains(string(out), "--masters=master") {
			t.Fatalf("%s: %s %v", ip, out, err)
		}
	}
}
func TestVectorAllInOneMasters(t *testing.T) {
	output, _ := render(t, "vector.enabled=true", "allInOne.enabled=true", "master.enabled=false")
	if !strings.Contains(output, "--masters=vector-test-seaweedfs-all-in-one.default:9333") {
		t.Fatal("vector cannot discover all-in-one master")
	}
	output, _ = render(t, "vector.enabled=true", "allInOne.enabled=true", "master.enabled=false", "vector.masters=external:9333")
	if !strings.Contains(output, "--masters=external:9333") {
		t.Fatal("explicit master override lost")
	}
}

func TestVectorNetworkPolicyOverrides(t *testing.T) {
	output, _ := render(t, "vector.enabled=true", "s3.enabled=true", "vector.createBuckets[0]=embeddings", "networkPolicy.enabled=true", "networkPolicy.components.vector.extraIngress[0].ports[0].port=9329", "networkPolicy.egress.enabled=true", "networkPolicy.components.vector-bucket-hook.extraEgress[0].ports[0].port=443")
	if !strings.Contains(output, "port: 9329") || !strings.Contains(output, "port: 443") {
		t.Fatal("vector network overrides missing")
	}
}

func TestVectorMetricsImageCompatibility(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", "template", "vector-test", "..", "--set", "vector.enabled=true", "--set", "vector.metricsPort=9329").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "default 4.53 image does not support metrics") {
		t.Fatalf("incompatible image accepted: %v %s", err, out)
	}
	render(t, "vector.enabled=true", "vector.metricsPort=9329", "vector.imageOverride=example/metrics-engine:test")
}
