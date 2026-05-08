package scheduler

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lifecycle_xml"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

const BucketLifecycleConfigurationXMLKey = "s3-bucket-lifecycle-configuration-xml"

// ParseError is returned alongside successfully-loaded inputs so callers can
// surface malformed bucket configs rather than silently dropping them.
type ParseError struct {
	Bucket string
	Err    error
}

// LoadCompileInputs walks the buckets directory and returns one
// engine.CompileInput per bucket that carries a non-empty lifecycle
// configuration. Pagination loops with startFrom so clusters with more
// than one page of buckets don't drop the tail.
func LoadCompileInputs(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketsPath string) ([]engine.CompileInput, []ParseError, error) {
	var (
		inputs      []engine.CompileInput
		parseErrors []ParseError
		startFrom   string
	)
	const pageSize uint32 = 1024
	for {
		pageCount := 0
		var lastName string
		err := filer_pb.SeaweedList(ctx, client, bucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
			pageCount++
			lastName = entry.Name
			if !entry.IsDirectory {
				return nil
			}
			xmlBytes, ok := entry.Extended[BucketLifecycleConfigurationXMLKey]
			if !ok || len(xmlBytes) == 0 {
				return nil
			}
			rules, err := lifecycle_xml.ParseCanonical(xmlBytes)
			if err != nil {
				parseErrors = append(parseErrors, ParseError{Bucket: entry.Name, Err: err})
				return nil
			}
			if len(rules) == 0 {
				return nil
			}
			inputs = append(inputs, engine.CompileInput{
				Bucket:    entry.Name,
				Rules:     rules,
				Versioned: IsBucketVersioned(entry),
			})
			return nil
		}, startFrom, false, pageSize)
		if err != nil {
			return nil, nil, err
		}
		if uint32(pageCount) < pageSize {
			break
		}
		startFrom = lastName
	}
	return inputs, parseErrors, nil
}

// IsBucketVersioned returns true when the bucket's filer entry has the
// versioning extended attribute set to Enabled or Suspended.
func IsBucketVersioned(entry *filer_pb.Entry) bool {
	v, ok := entry.Extended[s3_constants.ExtVersioningKey]
	if !ok {
		return false
	}
	s := strings.ToLower(strings.TrimSpace(string(v)))
	return s == "enabled" || s == "suspended"
}

// AllActivePriorStates seeds every compiled action as bootstrap-complete +
// event-driven so the scheduler dispatches whatever fires immediately.
// Production bootstrap walks set this incrementally per bucket; the
// scheduler runs out-of-band of that flow for now.
func AllActivePriorStates(inputs []engine.CompileInput) map[s3lifecycle.ActionKey]engine.PriorState {
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	for _, in := range inputs {
		for _, rule := range in.Rules {
			hash := s3lifecycle.RuleHash(rule)
			for _, kind := range s3lifecycle.RuleActionKinds(rule) {
				key := s3lifecycle.ActionKey{Bucket: in.Bucket, RuleHash: hash, ActionKind: kind}
				prior[key] = engine.PriorState{
					BootstrapComplete: true,
					Mode:              engine.ModeEventDriven,
				}
			}
		}
	}
	return prior
}
