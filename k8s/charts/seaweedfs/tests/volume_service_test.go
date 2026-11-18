package tests

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func findDocument(documents []map[string]interface{}, kind, name string) map[string]interface{} {
	for _, document := range documents {
		if document["kind"] == kind && document["metadata"].(map[string]interface{})["name"] == name {
			return document
		}
	}
	return nil
}

func TestVolumeServicePublishesNotReadyPods(t *testing.T) {
	_, documents := render(t)
	service := findDocument(documents, "Service", "vector-test-seaweedfs-volume")
	if service == nil {
		t.Fatal("no volume service")
	}
	if service["spec"].(map[string]interface{})["publishNotReadyAddresses"] != true {
		t.Fatal("volume pod DNS names must resolve before the pods are Ready")
	}
	if findDocument(documents, "Service", "vector-test-seaweedfs-client-volume") != nil {
		t.Fatal("client-volume service rendered without ingress or httpRoute")
	}
}

func TestVolumeIngressUsesReadyOnlyService(t *testing.T) {
	_, documents := render(t, "volume.ingress.enabled=true", "volume.httpRoute.enabled=true", "volume.metricsPort=9327")
	client := findDocument(documents, "Service", "vector-test-seaweedfs-client-volume")
	if client == nil {
		t.Fatal("no client-volume service")
	}
	spec := client["spec"].(map[string]interface{})
	if _, found := spec["publishNotReadyAddresses"]; found {
		t.Fatal("client-volume must publish only Ready pods")
	}
	for _, port := range spec["ports"].([]interface{}) {
		if port.(map[string]interface{})["name"] == "metrics" {
			t.Fatal("client-volume must not expose metrics, or the ServiceMonitor scrapes each pod twice")
		}
	}

	ingress := findDocument(documents, "Ingress", "ingress-vector-test-seaweedfs-volume")
	if ingress == nil {
		t.Fatal("no volume ingress")
	}
	path := ingress["spec"].(map[string]interface{})["rules"].([]interface{})[0].(map[string]interface{})["http"].(map[string]interface{})["paths"].([]interface{})[0].(map[string]interface{})
	if backend := path["backend"].(map[string]interface{})["service"].(map[string]interface{})["name"]; backend != "vector-test-seaweedfs-client-volume" {
		t.Fatalf("volume ingress backend is %v", backend)
	}

	route := findDocument(documents, "HTTPRoute", "vector-test-seaweedfs-volume")
	if route == nil {
		t.Fatal("no volume httpRoute")
	}
	rule := route["spec"].(map[string]interface{})["rules"].([]interface{})[0].(map[string]interface{})
	if backend := rule["backendRefs"].([]interface{})[0].(map[string]interface{})["name"]; backend != "vector-test-seaweedfs-client-volume" {
		t.Fatalf("volume httpRoute backend is %v", backend)
	}
}

func TestVolumeRoutingServiceDoesNotCollideWithVolumeGroups(t *testing.T) {
	_, documents := render(t, "volume.ingress.enabled=true", "volume.httpRoute.enabled=true", "volumes.client.replicas=1")
	seen := map[string]bool{}
	for _, document := range documents {
		key := fmt.Sprint(document["kind"], "/", document["metadata"].(map[string]interface{})["name"])
		if seen[key] {
			t.Fatalf("%s rendered twice", key)
		}
		seen[key] = true
	}
	if !seen["Service/vector-test-seaweedfs-volume-client"] || !seen["Service/vector-test-seaweedfs-client-volume"] {
		t.Fatal("volumes.client group Service and the routing Service must both render")
	}
}

func TestVolumeRoutingServiceRejectsTruncatedCollision(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	fullname := strings.Repeat("a", 49) + "-client"
	output, err := exec.Command("helm", "template", "test", "..", "--set", "fullnameOverride="+fullname, "--set", "volume.ingress.enabled=true").CombinedOutput()
	if err == nil || !strings.Contains(string(output), "same name as the volume Service") {
		t.Fatalf("colliding routing Service accepted: %v %s", err, output)
	}
}

func TestVolumeIngressAllInOne(t *testing.T) {
	_, documents := render(t, "allInOne.enabled=true", "volume.ingress.enabled=true")
	if findDocument(documents, "Service", "vector-test-seaweedfs-client-volume") != nil {
		t.Fatal("client-volume service rendered in all-in-one mode")
	}
	ingress := findDocument(documents, "Ingress", "ingress-vector-test-seaweedfs-volume")
	if ingress == nil {
		t.Fatal("no volume ingress")
	}
	path := ingress["spec"].(map[string]interface{})["rules"].([]interface{})[0].(map[string]interface{})["http"].(map[string]interface{})["paths"].([]interface{})[0].(map[string]interface{})
	if backend := path["backend"].(map[string]interface{})["service"].(map[string]interface{})["name"]; backend != "vector-test-seaweedfs-all-in-one" {
		t.Fatalf("all-in-one volume ingress backend is %v", backend)
	}
}
