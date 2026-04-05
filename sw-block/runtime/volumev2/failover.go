package volumev2

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
)

// FailoverParticipant is the legacy in-process convenience surface that
// predates the explicit failover adapter seam. It remains as a compatibility
// wrapper for tests and local callers while the adapter-backed target becomes
// the primary contract.
type FailoverParticipant interface {
	FailoverEvidenceAdapter
	FailoverTakeoverAdapter
}

// QueryReplicaSummarySource aliases the peer summary surface so the failover
// helper can reuse the existing bounded takeover contract.
type QueryReplicaSummarySource interface {
	ReplicaSummarySource
}

// FailoverResult captures the outputs of one authorized and prepared failover.
type FailoverResult struct {
	Candidate  masterv2.PromotionQueryResponse
	Assignment masterv2.Assignment
	Truth      ReconstructedPrimaryTruth
}

// FailoverStage is the coarse external progress marker for one failover
// session. It is intended for orchestration and debugging, not semantics.
type FailoverStage string

const (
	FailoverStageNew               FailoverStage = "new"
	FailoverStageEvidenceCollected FailoverStage = "evidence_collected"
	FailoverStageAuthorized        FailoverStage = "authorized"
	FailoverStagePrepared          FailoverStage = "prepared"
	FailoverStageActivated         FailoverStage = "activated"
	FailoverStageFailed            FailoverStage = "failed"
)

// FailoverSnapshot is a read-only summary of the session's current observable
// state for drivers, tests, and debug surfaces.
type FailoverSnapshot struct {
	VolumeName     string
	ExpectedEpoch  uint64
	Stage          FailoverStage
	LastError      string
	ResponseCount  int
	SelectedNodeID string
	Result         FailoverResult
}

// FailoverSession is a thin orchestration object that exposes the narrow
// failover stages explicitly so higher-level drivers can stop after
// authorization, inspect intermediate results, or run the whole sequence.
type FailoverSession struct {
	master        *masterv2.Master
	volumeName    string
	expectedEpoch uint64
	targets       []FailoverTarget

	responses []masterv2.PromotionQueryResponse
	byNode    map[string]FailoverTarget
	result    FailoverResult
	stage     FailoverStage
	lastErr   error
}

// NewFailoverSession validates the adapter-backed failover inputs and returns a
// stepwise orchestration session.
func NewFailoverSession(master *masterv2.Master, volumeName string, expectedEpoch uint64, targets []FailoverTarget) (*FailoverSession, error) {
	if master == nil {
		return nil, fmt.Errorf("volumev2: master is nil")
	}
	if volumeName == "" {
		return nil, fmt.Errorf("volumev2: volume name is required")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("volumev2: failover targets are required")
	}
	return &FailoverSession{
		master:        master,
		volumeName:    volumeName,
		expectedEpoch: expectedEpoch,
		targets:       targets,
		stage:         FailoverStageNew,
	}, nil
}

// NewFailoverSessionFromParticipants adapts the legacy participant surface into
// the explicit target seam for compatibility with older in-process tests.
func NewFailoverSessionFromParticipants(master *masterv2.Master, volumeName string, expectedEpoch uint64, participants []FailoverParticipant) (*FailoverSession, error) {
	targets := make([]FailoverTarget, 0, len(participants))
	for _, participant := range participants {
		if participant == nil {
			continue
		}
		resp, err := participant.QueryPromotionEvidence(masterv2.PromotionQueryRequest{
			VolumeName:    volumeName,
			ExpectedEpoch: expectedEpoch,
		})
		if err != nil {
			return nil, fmt.Errorf("volumev2: participant target discovery %s: %w", volumeName, err)
		}
		if resp.NodeID == "" {
			return nil, fmt.Errorf("volumev2: participant target discovery for %s missing node id", volumeName)
		}
		targets = append(targets, FailoverTarget{
			NodeID:   resp.NodeID,
			Evidence: participant,
			Takeover: participant,
		})
	}
	return NewFailoverSession(master, volumeName, expectedEpoch, targets)
}

// CollectPromotionEvidence gathers fresh promotion responses from all
// configured participants.
func (s *FailoverSession) CollectPromotionEvidence() ([]masterv2.PromotionQueryResponse, error) {
	if s == nil {
		return nil, fmt.Errorf("volumev2: failover session is nil")
	}
	responses := make([]masterv2.PromotionQueryResponse, 0, len(s.targets))
	byNode := make(map[string]FailoverTarget, len(s.targets))
	for _, target := range s.targets {
		if target.NodeID == "" || target.Evidence == nil {
			continue
		}
		resp, err := target.Evidence.QueryPromotionEvidence(masterv2.PromotionQueryRequest{
			VolumeName:    s.volumeName,
			ExpectedEpoch: s.expectedEpoch,
		})
		if err != nil {
			return nil, s.failf("volumev2: promotion evidence %s: %w", s.volumeName, err)
		}
		if resp.NodeID == "" {
			return nil, s.failf("volumev2: promotion evidence for %s missing node id", s.volumeName)
		}
		responses = append(responses, resp)
		byNode[resp.NodeID] = target
	}
	if len(responses) == 0 {
		return nil, s.failf("volumev2: no failover evidence collected for %s", s.volumeName)
	}
	s.responses = responses
	s.byNode = byNode
	s.stage = FailoverStageEvidenceCollected
	s.lastErr = nil
	return append([]masterv2.PromotionQueryResponse(nil), responses...), nil
}

// Authorize asks masterv2 to pick and authorize the new primary assignment from
// the collected fresh promotion evidence.
func (s *FailoverSession) Authorize() (masterv2.Assignment, error) {
	if s == nil {
		return masterv2.Assignment{}, fmt.Errorf("volumev2: failover session is nil")
	}
	if len(s.responses) == 0 {
		if _, err := s.CollectPromotionEvidence(); err != nil {
			return masterv2.Assignment{}, err
		}
	}
	assignment, err := s.master.AuthorizePromotion(s.volumeName, s.responses)
	if err != nil {
		return masterv2.Assignment{}, s.fail(err)
	}
	s.result.Assignment = assignment
	for _, resp := range s.responses {
		if resp.NodeID == assignment.NodeID {
			s.result.Candidate = resp
			break
		}
	}
	s.stage = FailoverStageAuthorized
	s.lastErr = nil
	return assignment, nil
}

// PrepareTakeover runs the selected node's bounded takeover reconstruction.
func (s *FailoverSession) PrepareTakeover() (ReconstructedPrimaryTruth, error) {
	if s == nil {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: failover session is nil")
	}
	if s.result.Assignment.NodeID == "" {
		if _, err := s.Authorize(); err != nil {
			return ReconstructedPrimaryTruth{}, err
		}
	}
	selected, ok := s.byNode[s.result.Assignment.NodeID]
	if !ok {
		return ReconstructedPrimaryTruth{}, s.failf("volumev2: authorized node %q missing participant", s.result.Assignment.NodeID)
	}
	peers := make([]ReplicaSummarySource, 0, len(s.byNode)-1)
	for nodeID, target := range s.byNode {
		if nodeID == s.result.Assignment.NodeID {
			continue
		}
		if target.Evidence != nil {
			peers = append(peers, target.Evidence)
		}
	}
	if selected.Takeover == nil {
		return ReconstructedPrimaryTruth{}, s.failf("volumev2: authorized node %q missing takeover adapter", s.result.Assignment.NodeID)
	}
	truth, err := selected.Takeover.PreparePrimaryTakeover(PrimaryTakeoverPlan{
		Assignment: s.result.Assignment,
		Peers:      peers,
	})
	s.result.Truth = truth
	if err != nil {
		return truth, s.fail(err)
	}
	s.stage = FailoverStagePrepared
	s.lastErr = nil
	return truth, nil
}

// Activate gates the selected primary on the reconstructed takeover truth.
func (s *FailoverSession) Activate() error {
	if s == nil {
		return fmt.Errorf("volumev2: failover session is nil")
	}
	if s.result.Assignment.NodeID == "" {
		if _, err := s.Authorize(); err != nil {
			return err
		}
	}
	if s.result.Truth.PrimaryNodeID == "" {
		if _, err := s.PrepareTakeover(); err != nil {
			return err
		}
	}
	selected, ok := s.byNode[s.result.Assignment.NodeID]
	if !ok {
		return s.failf("volumev2: authorized node %q missing participant", s.result.Assignment.NodeID)
	}
	if selected.Takeover == nil {
		return s.failf("volumev2: authorized node %q missing takeover adapter", s.result.Assignment.NodeID)
	}
	if err := selected.Takeover.GatePrimaryActivation(s.volumeName, s.result.Truth); err != nil {
		return s.fail(err)
	}
	s.stage = FailoverStageActivated
	s.lastErr = nil
	return nil
}

// Result returns the latest collected candidate, assignment, and reconstructed
// takeover truth known to the session.
func (s *FailoverSession) Result() FailoverResult {
	if s == nil {
		return FailoverResult{}
	}
	return s.result
}

// Stage returns the current coarse failover stage.
func (s *FailoverSession) Stage() FailoverStage {
	if s == nil {
		return FailoverStageFailed
	}
	return s.stage
}

// LastError returns the last stage error observed by the session.
func (s *FailoverSession) LastError() error {
	if s == nil {
		return fmt.Errorf("volumev2: failover session is nil")
	}
	return s.lastErr
}

// Snapshot returns a stable read-only view of the session's externally useful
// state.
func (s *FailoverSession) Snapshot() FailoverSnapshot {
	if s == nil {
		return FailoverSnapshot{Stage: FailoverStageFailed, LastError: "volumev2: failover session is nil"}
	}
	lastErr := ""
	if s.lastErr != nil {
		lastErr = s.lastErr.Error()
	}
	return FailoverSnapshot{
		VolumeName:     s.volumeName,
		ExpectedEpoch:  s.expectedEpoch,
		Stage:          s.stage,
		LastError:      lastErr,
		ResponseCount:  len(s.responses),
		SelectedNodeID: s.result.Assignment.NodeID,
		Result:         s.result,
	}
}

// Run executes the full failover path from fresh evidence collection through
// activation gating.
func (s *FailoverSession) Run() (FailoverResult, error) {
	if s == nil {
		return FailoverResult{}, fmt.Errorf("volumev2: failover session is nil")
	}
	if _, err := s.CollectPromotionEvidence(); err != nil {
		return s.Result(), err
	}
	if _, err := s.Authorize(); err != nil {
		return s.Result(), err
	}
	if _, err := s.PrepareTakeover(); err != nil {
		return s.Result(), err
	}
	if err := s.Activate(); err != nil {
		return s.Result(), err
	}
	return s.Result(), nil
}

// ExecuteFailoverFlow runs the narrow failover path:
// fresh promotion evidence -> master authorization -> takeover preparation ->
// activation gate. It intentionally does not choreograph catch-up or rebuild.
func ExecuteFailoverFlow(master *masterv2.Master, volumeName string, expectedEpoch uint64, targets []FailoverTarget) (FailoverResult, error) {
	session, err := NewFailoverSession(master, volumeName, expectedEpoch, targets)
	if err != nil {
		return FailoverResult{}, err
	}
	return session.Run()
}

// ExecuteFailoverFlowFromParticipants preserves the old in-process participant
// entry point while the explicit target seam becomes primary.
func ExecuteFailoverFlowFromParticipants(master *masterv2.Master, volumeName string, expectedEpoch uint64, participants []FailoverParticipant) (FailoverResult, error) {
	session, err := NewFailoverSessionFromParticipants(master, volumeName, expectedEpoch, participants)
	if err != nil {
		return FailoverResult{}, err
	}
	return session.Run()
}

func (s *FailoverSession) fail(err error) error {
	if s == nil {
		return err
	}
	s.stage = FailoverStageFailed
	s.lastErr = err
	return err
}

func (s *FailoverSession) failf(format string, args ...any) error {
	return s.fail(fmt.Errorf(format, args...))
}
