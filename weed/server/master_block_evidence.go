package weed_server

import (
	"context"
	"fmt"
	"time"
)

// BlockPromotionEvidenceQuerier queries one volume server for fresh promotion
// evidence at failover time. The default implementation will use gRPC once
// proto is regenerated. For testing, a direct in-process implementation is
// used.
type BlockPromotionEvidenceQuerier func(ctx context.Context, server, path string, expectedEpoch uint64) (BlockPromotionEvidence, error)

// queryBlockPromotionEvidence queries one candidate for fresh evidence with a
// bounded timeout. Returns the evidence or an error. The master must not
// promote based on stale/cached data — only fresh evidence from this call.
func queryBlockPromotionEvidence(querier BlockPromotionEvidenceQuerier, server, path string, expectedEpoch uint64) (BlockPromotionEvidence, error) {
	if querier == nil {
		return BlockPromotionEvidence{}, fmt.Errorf("promotion evidence querier is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return querier(ctx, server, path, expectedEpoch)
}

// queryAllCandidateEvidence queries multiple candidates in sequence and returns
// all successfully collected evidence. Failed queries are recorded as errors
// but do not stop collection from other candidates.
func queryAllCandidateEvidence(querier BlockPromotionEvidenceQuerier, candidates []promotionCandidate) ([]BlockPromotionEvidence, []error) {
	var evidence []BlockPromotionEvidence
	var errs []error
	for _, c := range candidates {
		ev, err := queryBlockPromotionEvidence(querier, c.server, c.path, c.expectedEpoch)
		if err != nil {
			errs = append(errs, fmt.Errorf("evidence query %s %s: %w", c.server, c.path, err))
			continue
		}
		ev.Server = c.server
		evidence = append(evidence, ev)
	}
	return evidence, errs
}

// selectDurabilityFirstCandidate selects the best promotion candidate from
// fresh evidence using durability-first ordering:
//
//  1. Filter: only eligible candidates
//  2. Rank: highest CommittedLSN
//  3. Tie-break: highest WALHeadLSN
//  4. Tie-break: highest HealthScore
//
// Returns an error if no eligible candidate exists (fail-closed).
func selectDurabilityFirstCandidate(evidence []BlockPromotionEvidence) (BlockPromotionEvidence, error) {
	var eligible []BlockPromotionEvidence
	for _, ev := range evidence {
		if ev.Eligible {
			eligible = append(eligible, ev)
		}
	}
	if len(eligible) == 0 {
		return BlockPromotionEvidence{}, fmt.Errorf("no eligible promotion candidate: %d queried, 0 eligible", len(evidence))
	}
	best := eligible[0]
	for _, ev := range eligible[1:] {
		if ev.CommittedLSN > best.CommittedLSN {
			best = ev
		} else if ev.CommittedLSN == best.CommittedLSN {
			if ev.WALHeadLSN > best.WALHeadLSN {
				best = ev
			} else if ev.WALHeadLSN == best.WALHeadLSN {
				if ev.HealthScore > best.HealthScore {
					best = ev
				}
			}
		}
	}
	return best, nil
}

// promotionCandidate is the minimal info needed to query one candidate.
type promotionCandidate struct {
	server        string
	path          string
	expectedEpoch uint64
}
