package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for LoadCompileInputs / IsBucketVersioned / AllActivePriorStates.
// LoadCompileInputs is the bridge between the filer's bucket directory
// and the engine snapshot the scheduler compiles every refresh —
// pagination, parse-error surfacing, and the empty/missing-XML skip
// semantics matter because a missed bucket silently disables lifecycle
// for that prefix until the next refresh.

const testBucketsRoot = "/buckets"

// minimalLifecycleXML is the smallest XML that ParseCanonical accepts as
// a non-empty rule set; the actual rule details are exercised by the
// lifecycle_xml package's own tests.
const minimalLifecycleXML = `<LifecycleConfiguration>
  <Rule>
    <ID>r1</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

func bucketEntry(name string, extended map[string][]byte) *filer_pb.Entry {
	return dirEntry(name, extended)
}

func TestIsBucketVersioned_MissingExtendedReturnsFalse(t *testing.T) {
	assert.False(t, IsBucketVersioned(&filer_pb.Entry{}))
	assert.False(t, IsBucketVersioned(&filer_pb.Entry{Extended: map[string][]byte{}}))
}

func TestIsBucketVersioned_AcceptsEnabledAndSuspended(t *testing.T) {
	// Casing and surrounding whitespace are normalized so the worker
	// tolerates whatever casing the writer chose.
	for _, raw := range []string{"Enabled", "ENABLED", "enabled", "  enabled  ", "Suspended", "suspended"} {
		t.Run(raw, func(t *testing.T) {
			e := &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtVersioningKey: []byte(raw)}}
			assert.True(t, IsBucketVersioned(e), "value %q must classify as versioned", raw)
		})
	}
}

func TestIsBucketVersioned_RejectsOtherValues(t *testing.T) {
	for _, raw := range []string{"", "Disabled", "Off", "true", "1", "garbage"} {
		t.Run(raw, func(t *testing.T) {
			e := &filer_pb.Entry{Extended: map[string][]byte{s3_constants.ExtVersioningKey: []byte(raw)}}
			assert.False(t, IsBucketVersioned(e), "value %q must NOT classify as versioned", raw)
		})
	}
}

func TestAllActivePriorStates_EmptyInputs(t *testing.T) {
	assert.Empty(t, AllActivePriorStates(nil))
	assert.Empty(t, AllActivePriorStates([]engine.CompileInput{}))
}

func TestAllActivePriorStates_SeedsAllActionsAsActive(t *testing.T) {
	// Every (bucket, ruleHash, actionKind) tuple from the inputs must
	// receive a PriorState with BootstrapComplete=true and
	// Mode=ModeEventDriven so the scheduler dispatches matched events
	// immediately rather than waiting for a per-action bootstrap walk.
	rule := &s3lifecycle.Rule{
		ID:             "r1",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	inputs := []engine.CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}
	prior := AllActivePriorStates(inputs)

	hash := s3lifecycle.RuleHash(rule)
	kinds := s3lifecycle.RuleActionKinds(rule)
	require.NotEmpty(t, kinds, "expiration-days rule must yield at least one action kind")
	for _, kind := range kinds {
		key := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: hash, ActionKind: kind}
		state, ok := prior[key]
		require.True(t, ok, "prior state missing for action kind %v", kind)
		assert.True(t, state.BootstrapComplete, "bootstrap complete for kind %v", kind)
		assert.Equal(t, engine.ModeEventDriven, state.Mode, "mode for kind %v", kind)
	}
	assert.Len(t, prior, len(kinds), "exactly one prior-state entry per action kind")
}

func TestAllActivePriorStates_KeysSeparatedByBucket(t *testing.T) {
	// Two buckets sharing identical rules must still get distinct
	// PriorState entries, otherwise refresh would conflate their state.
	rule := &s3lifecycle.Rule{ID: "r1", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30}
	inputs := []engine.CompileInput{
		{Bucket: "alpha", Rules: []*s3lifecycle.Rule{rule}},
		{Bucket: "beta", Rules: []*s3lifecycle.Rule{rule}},
	}
	prior := AllActivePriorStates(inputs)
	hash := s3lifecycle.RuleHash(rule)
	for _, kind := range s3lifecycle.RuleActionKinds(rule) {
		assert.Contains(t, prior, s3lifecycle.ActionKey{Bucket: "alpha", RuleHash: hash, ActionKind: kind})
		assert.Contains(t, prior, s3lifecycle.ActionKey{Bucket: "beta", RuleHash: hash, ActionKind: kind})
	}
}

func TestLoadCompileInputs_EmptyBucketDir(t *testing.T) {
	// No buckets at all -> no inputs, no parse errors, no transport error.
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{testBucketsRoot: {}}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, inputs)
	assert.Empty(t, perr)
}

func TestLoadCompileInputs_FilesAtBucketLevelSkipped(t *testing.T) {
	// A bare file at the buckets root isn't a bucket; the loader must
	// skip it without touching the parser.
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {fileEntry("stray.txt")},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, inputs)
	assert.Empty(t, perr)
}

func TestLoadCompileInputs_BucketWithoutLifecycleConfigSkipped(t *testing.T) {
	// Buckets that don't carry the lifecycle XML extended attribute are
	// silently skipped — operators may have many buckets without rules.
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {bucketEntry("b1", nil)},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, inputs)
	assert.Empty(t, perr)
}

func TestLoadCompileInputs_BucketWithEmptyLifecycleXMLSkipped(t *testing.T) {
	// Empty bytes under the lifecycle key shouldn't reach the parser.
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {bucketEntry("b1", map[string][]byte{
			BucketLifecycleConfigurationXMLKey: nil,
		})},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, inputs)
	assert.Empty(t, perr)
}

func TestLoadCompileInputs_ValidConfigBecomesInput(t *testing.T) {
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {bucketEntry("b1", map[string][]byte{
			BucketLifecycleConfigurationXMLKey: []byte(minimalLifecycleXML),
		})},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, perr)
	require.Len(t, inputs, 1)
	assert.Equal(t, "b1", inputs[0].Bucket)
	assert.False(t, inputs[0].Versioned, "no versioning attr -> not versioned")
	assert.NotEmpty(t, inputs[0].Rules)
}

func TestLoadCompileInputs_VersioningAttrPropagates(t *testing.T) {
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {bucketEntry("b1", map[string][]byte{
			BucketLifecycleConfigurationXMLKey: []byte(minimalLifecycleXML),
			s3_constants.ExtVersioningKey:      []byte("Enabled"),
		})},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, perr)
	require.Len(t, inputs, 1)
	assert.True(t, inputs[0].Versioned)
}

func TestLoadCompileInputs_MalformedXMLBecomesParseError(t *testing.T) {
	// Malformed XML must not abort the whole walk — surfacing it as a
	// ParseError lets operators see the bucket-level failure while
	// other buckets keep loading.
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: {
			bucketEntry("bad", map[string][]byte{
				BucketLifecycleConfigurationXMLKey: []byte("<not-valid-xml"),
			}),
			bucketEntry("good", map[string][]byte{
				BucketLifecycleConfigurationXMLKey: []byte(minimalLifecycleXML),
			}),
		},
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	require.Len(t, perr, 1)
	assert.Equal(t, "bad", perr[0].Bucket)
	assert.Error(t, perr[0].Err)

	require.Len(t, inputs, 1)
	assert.Equal(t, "good", inputs[0].Bucket)
}

func TestLoadCompileInputs_PaginatesAcrossPages(t *testing.T) {
	// pageSize is 1024 inside LoadCompileInputs; populate just over the
	// boundary to force a second listing call. Every bucket has the same
	// minimal config so the input count must equal the bucket count.
	const total = 1030
	entries := make([]*filer_pb.Entry, 0, total)
	for i := 0; i < total; i++ {
		entries = append(entries, bucketEntry(fmt.Sprintf("b%05d", i), map[string][]byte{
			BucketLifecycleConfigurationXMLKey: []byte(minimalLifecycleXML),
		}))
	}
	client := &fakeFilerClient{tree: map[string][]*filer_pb.Entry{
		testBucketsRoot: entries,
	}}
	inputs, perr, err := LoadCompileInputs(context.Background(), client, testBucketsRoot)
	require.NoError(t, err)
	assert.Empty(t, perr)
	require.Len(t, inputs, total, "pagination must surface every bucket")
	// Order is preserved across pages, and the page boundary at 1023/1024
	// (pageSize is 1024) must not skip or duplicate entries.
	assert.Equal(t, "b00000", inputs[0].Bucket)
	assert.Equal(t, "b01023", inputs[1023].Bucket, "last entry of first page")
	assert.Equal(t, "b01024", inputs[1024].Bucket, "first entry of second page")
	assert.Equal(t, fmt.Sprintf("b%05d", total-1), inputs[total-1].Bucket)
}
