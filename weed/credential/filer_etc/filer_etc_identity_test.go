package filer_etc

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putEntry(t *testing.T, server *policyTestFilerServer, dir, name string, content []byte) {
	t.Helper()
	_, err := server.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry:     &filer_pb.Entry{Name: name, Content: content},
	})
	require.NoError(t, err)
}

// A mid-rewrite identity file (empty content, as seen while a secrets tool
// truncates and rewrites the file) must fail the snapshot instead of being
// silently dropped. Silently dropping it would make a full reload install an
// incomplete identity set and deny unrelated clients mid-reload.
func TestLoadConfigurationFailsOnEmptyIdentityFile(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	identDir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
	putEntry(t, server, identDir, "alice.json", []byte(`{"name":"alice","credentials":[{"accessKey":"AK","secretKey":"SK"}]}`))
	putEntry(t, server, identDir, "bob.json", []byte{}) // mid-rewrite: empty

	_, err := store.LoadConfiguration(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bob.json")
}

// A malformed identity file (partial JSON from a mid-rewrite) must also fail
// the snapshot rather than being skipped.
func TestLoadConfigurationFailsOnMalformedIdentityFile(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	identDir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
	putEntry(t, server, identDir, "alice.json", []byte(`{"name":"alice"}`))
	putEntry(t, server, identDir, "bob.json", []byte(`{"name":"bob"`)) // truncated

	_, err := store.LoadConfiguration(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bob.json")
}

// A mid-rewrite policy file must fail the snapshot rather than be dropped.
func TestLoadManagedPoliciesFailsOnEmptyPolicyFile(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	polDir := filer.IamConfigDirectory + "/" + IamPoliciesDirectory
	putEntry(t, server, polDir, "good.json", []byte(`{"Version":"2012-10-17","Statement":[]}`))
	putEntry(t, server, polDir, "bad.json", []byte{}) // mid-rewrite: empty

	_, err := store.LoadManagedPolicies(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad.json")
}

// A non-JSON auxiliary file (README, .DS_Store, a migration backup) in an IAM
// directory must not fail the snapshot: only *.json files are IAM objects.
func TestLoadConfigurationIgnoresNonJsonAuxiliaryFiles(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	identDir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
	putEntry(t, server, identDir, "alice.json", []byte(`{"name":"alice","credentials":[{"accessKey":"AK","secretKey":"SK"}]}`))
	putEntry(t, server, identDir, "README.txt", []byte(`not an identity`))
	putEntry(t, server, identDir, "alice.json.old", []byte(`{"name":"alice"}`))

	polDir := filer.IamConfigDirectory + "/" + IamPoliciesDirectory
	putEntry(t, server, polDir, "p.json", []byte(`{"Version":"2012-10-17","Statement":[]}`))
	putEntry(t, server, polDir, "notes.md", []byte(`# policies`))

	cfg, err := store.LoadConfiguration(ctx)
	require.NoError(t, err)
	require.Len(t, cfg.Identities, 1)
	assert.Equal(t, "alice", cfg.Identities[0].Name)
}

// A valid JSON file with an empty name (e.g. `{}`) would unmarshal cleanly but
// install a garbage empty-key record that can displace a real one. Reject it.
func TestLoadConfigurationFailsOnEmptyIdentityName(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	identDir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
	putEntry(t, server, identDir, "alice.json", []byte(`{"name":"alice"}`))
	putEntry(t, server, identDir, "empty.json", []byte(`{}`))

	_, err := store.LoadConfiguration(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty.json")
}

func TestLoadConfigurationFailsOnEmptyGroupName(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	groupDir := filer.IamConfigDirectory + "/" + IamGroupsDirectory
	putEntry(t, server, groupDir, "empty.json", []byte(`{}`))

	_, err := store.LoadConfiguration(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty.json")
}

func TestLoadConfigurationFailsOnEmptyServiceAccountId(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	saDir := filer.IamConfigDirectory + "/" + IamServiceAccountsDirectory
	putEntry(t, server, saDir, "empty.json", []byte(`{}`))

	_, err := store.LoadConfiguration(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty.json")
}

// A valid full snapshot loads without error (regression guard).
func TestLoadConfigurationSucceedsOnValidFiles(t *testing.T) {
	ctx := context.Background()
	store, server := newPolicyTestStoreWithServer(t)

	identDir := filer.IamConfigDirectory + "/" + IamIdentitiesDirectory
	putEntry(t, server, identDir, "alice.json", []byte(`{"name":"alice","credentials":[{"accessKey":"AK","secretKey":"SK"}]}`))
	putEntry(t, server, identDir, "bob.json", []byte(`{"name":"bob","credentials":[{"accessKey":"BK","secretKey":"SK"}]}`))

	cfg, err := store.LoadConfiguration(ctx)
	require.NoError(t, err)
	names := make([]string, 0, len(cfg.Identities))
	for _, id := range cfg.Identities {
		names = append(names, id.Name)
	}
	assert.ElementsMatch(t, []string{"alice", "bob"}, names)
}
