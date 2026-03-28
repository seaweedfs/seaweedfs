package volumefsm

import (
	"fmt"

	fsmv2 "github.com/seaweedfs/seaweedfs/sw-block/prototype/fsmv2"
)

type ScenarioStep struct {
	Name  string
	Event Event
}

type ReplicaSnapshot struct {
	State      fsmv2.State
	FlushedLSN uint64
}

type Snapshot struct {
	Step         string
	Epoch        uint64
	PrimaryID    string
	PrimaryState PrimaryState
	HeadLSN      uint64
	WriteGate    AdmissionDecision
	AckGate      AdmissionDecision
	Replicas     map[string]ReplicaSnapshot
}

func (m *Model) Snapshot(step string) Snapshot {
	replicas := make(map[string]ReplicaSnapshot, len(m.Replicas))
	for id, r := range m.Replicas {
		replicas[id] = ReplicaSnapshot{
			State:      r.FSM.State,
			FlushedLSN: r.FSM.ReplicaFlushedLSN,
		}
	}
	return Snapshot{
		Step:         step,
		Epoch:        m.Epoch,
		PrimaryID:    m.PrimaryID,
		PrimaryState: m.PrimaryState,
		HeadLSN:      m.HeadLSN,
		WriteGate:    m.WriteAdmission(),
		AckGate:      m.AckAdmission(m.HeadLSN),
		Replicas:     replicas,
	}
}

func RunScenario(m *Model, steps []ScenarioStep) ([]Snapshot, error) {
	trace := make([]Snapshot, 0, len(steps)+1)
	trace = append(trace, m.Snapshot("initial"))
	for _, step := range steps {
		if err := m.Apply(step.Event); err != nil {
			return trace, fmt.Errorf("scenario step %q: %w", step.Name, err)
		}
		trace = append(trace, m.Snapshot(step.Name))
	}
	return trace, nil
}

