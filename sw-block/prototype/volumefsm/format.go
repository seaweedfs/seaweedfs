package volumefsm

import (
	"fmt"
	"sort"
	"strings"
)

func FormatSnapshot(s Snapshot) string {
	ids := make([]string, 0, len(s.Replicas))
	for id := range s.Replicas {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	parts := []string{
		fmt.Sprintf("step=%s", s.Step),
		fmt.Sprintf("epoch=%d", s.Epoch),
		fmt.Sprintf("primary=%s/%s", s.PrimaryID, s.PrimaryState),
		fmt.Sprintf("head=%d", s.HeadLSN),
		fmt.Sprintf("write=%t:%s", s.WriteGate.Allowed, s.WriteGate.Reason),
		fmt.Sprintf("ack=%t:%s", s.AckGate.Allowed, s.AckGate.Reason),
	}
	for _, id := range ids {
		r := s.Replicas[id]
		parts = append(parts, fmt.Sprintf("%s=%s@%d", id, r.State, r.FlushedLSN))
	}
	return strings.Join(parts, " ")
}

func FormatTrace(trace []Snapshot) string {
	lines := make([]string, 0, len(trace))
	for _, s := range trace {
		lines = append(lines, FormatSnapshot(s))
	}
	return strings.Join(lines, "\n")
}

