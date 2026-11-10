package command

import (
	"strings"
	"testing"
)

func TestMiniVectorRejectsExplicitListenerConflict(t *testing.T) {
	oldVector, oldPort, oldMetrics, oldFlags := *miniVector, *miniVectorPort, *miniVectorMetricsPort, explicitFlags
	defer func() {
		*miniVector, *miniVectorPort, *miniVectorMetricsPort, explicitFlags = oldVector, oldPort, oldMetrics, oldFlags
	}()
	*miniVector = true
	*miniVectorPort = 8090
	*miniVectorMetricsPort = 8090
	explicitFlags = map[string]bool{"vector.port": true, "vector.metricsPort": true}
	err := ensureAllPortsAvailableOnIP("127.0.0.1", false)
	if err == nil || !strings.Contains(err.Error(), "shared by explicit flags vector.port and vector.metricsPort") {
		t.Fatalf("listener conflict not rejected: %v", err)
	}
}
