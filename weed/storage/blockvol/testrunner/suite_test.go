package testrunner

import "testing"

func TestParseSuite_Valid(t *testing.T) {
	yaml := `
name: test-suite
topology:
  nodes:
    m01: {host: 192.168.1.1, user: test, key: /tmp/key}
deploy:
  kill_ports: [9433]
  binaries:
    - local: weed-linux
      remote: /opt/weed
scenarios:
  - path: scenarios/test.yaml
    id: T1
evidence:
  save_to: results/test
`
	suite, err := ParseSuite([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if suite.Name != "test-suite" {
		t.Fatalf("name=%q", suite.Name)
	}
	if len(suite.Scenarios) != 1 {
		t.Fatalf("scenarios=%d", len(suite.Scenarios))
	}
	if suite.Scenarios[0].ID != "T1" {
		t.Fatalf("id=%q", suite.Scenarios[0].ID)
	}
	if len(suite.Deploy.KillPorts) != 1 || suite.Deploy.KillPorts[0] != 9433 {
		t.Fatalf("kill_ports=%v", suite.Deploy.KillPorts)
	}
}

func TestParseSuite_MissingName(t *testing.T) {
	_, err := ParseSuite([]byte(`scenarios: [{path: x.yaml}]`))
	if err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestParseSuite_NoScenarios(t *testing.T) {
	_, err := ParseSuite([]byte(`name: empty`))
	if err == nil {
		t.Fatal("expected error for no scenarios")
	}
}
