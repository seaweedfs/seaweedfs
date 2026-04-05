package volumev2

import "fmt"

// Orchestrator closes the MVP control loop:
// heartbeat -> assignments -> local apply.
type Orchestrator struct {
	node    *Node
	session ControlSession
}

// NewOrchestrator creates a small volumev2 control/data orchestrator.
func NewOrchestrator(node *Node, session ControlSession) (*Orchestrator, error) {
	if node == nil {
		return nil, fmt.Errorf("volumev2: node is nil")
	}
	if session == nil {
		return nil, fmt.Errorf("volumev2: control session is nil")
	}
	return &Orchestrator{node: node, session: session}, nil
}

// SyncOnce reports one heartbeat and applies any returned assignments.
func (o *Orchestrator) SyncOnce() error {
	if o == nil || o.node == nil || o.session == nil {
		return fmt.Errorf("volumev2: orchestrator is not initialized")
	}
	hb, err := o.node.Heartbeat()
	if err != nil {
		return err
	}
	assignments, err := o.session.Heartbeat(hb)
	if err != nil {
		return err
	}
	return o.node.ApplyAssignments(assignments)
}
