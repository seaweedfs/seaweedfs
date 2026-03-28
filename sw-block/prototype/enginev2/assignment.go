package enginev2

// AssignmentIntent represents a coordinator-driven assignment update.
// It specifies the desired replica set and which replicas need recovery.
type AssignmentIntent struct {
	Endpoints       map[string]Endpoint    // desired replica set
	Epoch           uint64                 // current epoch
	RecoveryTargets map[string]SessionKind // replicas that need recovery (nil = no recovery)
}

// AssignmentResult records what the SenderGroup did in response to an assignment.
type AssignmentResult struct {
	Added              []string // new senders created
	Removed            []string // old senders stopped
	SessionsCreated    []string // fresh recovery sessions attached
	SessionsSuperseded []string // existing sessions superseded by new ones
	SessionsFailed     []string // recovery sessions that couldn't be created
}

// ApplyAssignment processes a coordinator assignment intent:
//  1. Reconcile endpoints — add/remove/update senders
//  2. For each recovery target, create a recovery session on the sender
//
// Epoch fencing: if intent.Epoch < sender.Epoch for any target, that target
// is rejected. Stale assignment intent cannot create live sessions.
func (sg *SenderGroup) ApplyAssignment(intent AssignmentIntent) AssignmentResult {
	var result AssignmentResult

	// Step 1: reconcile topology.
	result.Added, result.Removed = sg.Reconcile(intent.Endpoints, intent.Epoch)

	// Step 2: create recovery sessions for designated targets.
	if intent.RecoveryTargets == nil {
		return result
	}

	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for replicaID, kind := range intent.RecoveryTargets {
		sender, ok := sg.senders[replicaID]
		if !ok {
			result.SessionsFailed = append(result.SessionsFailed, replicaID)
			continue
		}
		// Reject stale assignment: intent epoch must match sender epoch.
		if intent.Epoch < sender.Epoch {
			result.SessionsFailed = append(result.SessionsFailed, replicaID)
			continue
		}
		_, err := sender.AttachSession(intent.Epoch, kind)
		if err != nil {
			// Session already active at current epoch — supersede it.
			sess := sender.SupersedeSession(kind, "assignment_intent")
			if sess != nil {
				result.SessionsSuperseded = append(result.SessionsSuperseded, replicaID)
			} else {
				result.SessionsFailed = append(result.SessionsFailed, replicaID)
			}
			continue
		}
		result.SessionsCreated = append(result.SessionsCreated, replicaID)
	}
	return result
}
