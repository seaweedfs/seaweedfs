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
