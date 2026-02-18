package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestBuildJobSpecFromProposalDoesNotReuseProposalID(t *testing.T) {
	t.Parallel()

	proposal := &plugin_pb.JobProposal{
		ProposalId: "vacuum-2",
		DedupeKey:  "vacuum:2",
		JobType:    "vacuum",
	}

	jobA := buildJobSpecFromProposal("vacuum", proposal, 0)
	jobB := buildJobSpecFromProposal("vacuum", proposal, 1)

	if jobA.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobA.JobId)
	}
	if jobB.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobB.JobId)
	}
	if jobA.JobId == jobB.JobId {
		t.Fatalf("job ids must be unique across jobs: %s", jobA.JobId)
	}
	if jobA.DedupeKey != proposal.DedupeKey {
		t.Fatalf("dedupe key must be preserved: got=%s want=%s", jobA.DedupeKey, proposal.DedupeKey)
	}
}
