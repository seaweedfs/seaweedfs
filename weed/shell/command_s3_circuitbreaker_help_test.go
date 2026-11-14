package shell

import (
	"bytes"
	"strings"
	"testing"
)

// Every example in the s3.circuitBreaker help text has to run as written.
func TestCircuitBreakerHelpExamplesRun(t *testing.T) {
	originalLoadConfig := LoadConfig
	t.Cleanup(func() { LoadConfig = originalLoadConfig })
	LoadConfig = func(*CommandEnv, string, string, *bytes.Buffer) error { return nil }

	cmd := &commandS3CircuitBreaker{}
	examples := 0
	for _, line := range strings.Split(cmd.Help(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != cmd.Name() {
			continue
		}
		examples++
		// Leave out -apply: it saves the result inside the filer.
		var args []string
		for _, f := range fields[1:] {
			if f != "-apply" {
				args = append(args, f)
			}
		}
		var out bytes.Buffer
		if err := cmd.Do(args, nil, &out); err != nil {
			t.Errorf("%s: %v", strings.TrimSpace(line), err)
		}
	}
	if examples == 0 {
		t.Fatal("found no examples in the help text")
	}
}

func TestCircuitBreakerInvalidActionError(t *testing.T) {
	originalLoadConfig := LoadConfig
	t.Cleanup(func() { LoadConfig = originalLoadConfig })
	LoadConfig = func(*CommandEnv, string, string, *bytes.Buffer) error { return nil }

	cmd := &commandS3CircuitBreaker{}
	err := cmd.Do([]string{"-global", "-type", "Count", "-actions", "Read,Bogus", "-values", "1,2"}, nil, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected an error for an unknown action")
	}
	if msg := err.Error(); !strings.Contains(msg, "value(Bogus)") || !strings.Contains(msg, "flag[-actions]") {
		t.Errorf("error should name the bad action and -actions: %v", err)
	}
}
