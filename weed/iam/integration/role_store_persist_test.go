package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// persistentTestRoleStore stands in for a role store that outlives the process
// and may be shared (the filer store): anything but *MemoryRoleStore is
// treated as persistent.
type persistentTestRoleStore struct {
	*MemoryRoleStore
	cleared int
}

func (s *persistentTestRoleStore) ClearCache() { s.cleared++ }

// startRoleServer initializes a manager as an S3 server would at boot, with the
// given role store and config file roles.
func startRoleServer(t *testing.T, store RoleStore, staticRoles ...string) *IAMManager {
	t.Helper()
	mgr := NewIAMManager()
	require.NoError(t, mgr.Initialize(persistTestConfig(), func() string { return "localhost:8888" }))
	mgr.roleStore = store
	var defs []*RoleDefinition
	for _, name := range staticRoles {
		defs = append(defs, &RoleDefinition{RoleName: name})
	}
	mgr.LoadStaticRoles(context.Background(), defs)
	return mgr
}

func roleNames(t *testing.T, store RoleStore) map[string]bool {
	t.Helper()
	names, err := store.ListRoles(context.Background(), "")
	require.NoError(t, err)
	out := map[string]bool{}
	for _, n := range names {
		out[n] = true
	}
	return out
}

// A persistent store never receives the config file's roles: it outlives the
// file and may be shared with servers whose files differ.
func TestPersistentRoleStoreNeverHoldsConfigFileRoles(t *testing.T) {
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	mgr := startRoleServer(t, store, "from-file")

	assert.Empty(t, roleNames(t, store), "a config-file role was written to the persistent store")
	assert.True(t, roleNames(t, mgr.GetRoleStore())["from-file"], "the config-file role is not listed")
	role, err := mgr.GetRole(context.Background(), "from-file")
	require.NoError(t, err)
	assert.Equal(t, RoleSourceStaticConfig, role.Source)
	assert.Equal(t, StaticRoleID(&RoleDefinition{RoleName: "from-file"}), role.RoleId)
}

// Servers sharing a store, with different config files, must not remove or
// replace each other's roles — including a zero-config server.
func TestServersSharingARoleStoreKeepEachOthersRoles(t *testing.T) {
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	configured := startRoleServer(t, store, "from-file")
	require.NoError(t, configured.CreateRole(context.Background(), "", "created-at-runtime", &RoleDefinition{}))

	zeroConfig := startRoleServer(t, store)

	assert.True(t, roleNames(t, store)["created-at-runtime"], "a peer's start removed a role created at runtime")
	assert.True(t, roleNames(t, configured.GetRoleStore())["from-file"], "the configured server lost its config-file role")
	_, err := zeroConfig.GetRole(context.Background(), "from-file")
	assert.ErrorIs(t, err, ErrRoleNotFound, "a server sees a role only its peer's config file defines")
	_, err = zeroConfig.GetRole(context.Background(), "created-at-runtime")
	assert.NoError(t, err, "the peer does not see the shared role created at runtime")
}

// Removing a role from the config file removes it at the next start.
func TestRemovingARoleFromTheConfigFileRemovesIt(t *testing.T) {
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	startRoleServer(t, store, "from-file")

	restarted := startRoleServer(t, store) // the file no longer lists it

	_, err := restarted.GetRole(context.Background(), "from-file")
	assert.ErrorIs(t, err, ErrRoleNotFound, "a role removed from the config file is still served")
}

// The store refuses to replace or delete a role the config file defines; the
// file is where it changes.
func TestConfigFileRolesCannotBeReplacedOrDeletedThroughTheStore(t *testing.T) {
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	mgr := startRoleServer(t, store, "from-file")

	err := mgr.CreateRole(context.Background(), "", "from-file", &RoleDefinition{})
	assert.ErrorIs(t, err, ErrRoleStatic)
	assert.ErrorIs(t, mgr.DeleteRole(context.Background(), "from-file"), ErrRoleStatic)
	assert.Empty(t, roleNames(t, store), "a refused change still wrote to the store")
}

// An in-memory store keeps its behaviour: the config file's roles are records
// in it.
func TestInMemoryRoleStoreStillHoldsConfigFileRoles(t *testing.T) {
	store := NewMemoryRoleStore()
	startRoleServer(t, store, "from-file")
	assert.True(t, roleNames(t, store)["from-file"])
}

// Invalidating the role cache reaches the store beneath the config-file roles.
func TestRoleCacheInvalidationReachesTheStoreBeneathTheOverlay(t *testing.T) {
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	mgr := startRoleServer(t, store, "from-file")
	mgr.InvalidateRoleCache()
	assert.Equal(t, 1, store.cleared)
}

func TestRoleCopiesKeepSource(t *testing.T) {
	role := &RoleDefinition{RoleName: "r", Source: RoleSourceStaticConfig}
	assert.Equal(t, RoleSourceStaticConfig, copyRoleDefinition(role).Source)
	assert.Equal(t, RoleSourceStaticConfig, genericCopyRoleDefinition(role).Source)
}

// Config-file roles report no creation time: a time taken at load would
// change with every restart.
func TestConfigFileRolesReportNoCreationTime(t *testing.T) {
	for name, store := range map[string]RoleStore{
		"persistent": &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()},
		"in-memory":  NewMemoryRoleStore(),
	} {
		t.Run(name, func(t *testing.T) {
			mgr := startRoleServer(t, store, "from-file")
			role, err := mgr.GetRole(context.Background(), "from-file")
			require.NoError(t, err)
			assert.True(t, role.CreatedAt.IsZero())
		})
	}
}

// A role stored under a config-file role's name takes precedence, as a stored
// OIDC provider does; it can be changed, and deleting it brings the
// config-file role back.
func TestStoredRoleTakesPrecedenceOverTheConfigFileOne(t *testing.T) {
	ctx := context.Background()
	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	configured := startRoleServer(t, store, "shared-name")
	peer := startRoleServer(t, store)
	require.NoError(t, peer.CreateRole(ctx, "", "shared-name", &RoleDefinition{Description: "stored"}))

	role, err := configured.GetRole(ctx, "shared-name")
	require.NoError(t, err)
	assert.Equal(t, "stored", role.Description, "the config-file role hides the stored one")

	role.Description = "changed"
	assert.NoError(t, configured.CreateRole(ctx, "", "shared-name", role), "the stored role cannot be changed")

	require.NoError(t, configured.DeleteRole(ctx, "shared-name"))
	role, err = configured.GetRole(ctx, "shared-name")
	require.NoError(t, err)
	assert.Equal(t, RoleSourceStaticConfig, role.Source, "deleting the stored role did not bring the config-file one back")
}

// A store installed through SetRoleStore behaves like one installed at
// startup: the config-file roles stay visible and protected, and it serves
// the roles it holds.
func TestSetRoleStoreInstallsLikeStartup(t *testing.T) {
	mgr := startRoleServer(t, &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}, "from-file")

	store := &persistentTestRoleStore{MemoryRoleStore: NewMemoryRoleStore()}
	require.NoError(t, store.StoreRole(context.Background(), "", "stored", &RoleDefinition{RoleName: "stored"}))
	mgr.SetRoleStore(store)

	names := roleNames(t, mgr.GetRoleStore())
	assert.True(t, names["from-file"], "the config-file role is no longer listed")
	assert.True(t, names["stored"], "the new store's role is not listed")
	assert.ErrorIs(t, mgr.GetRoleStore().DeleteRole(context.Background(), "", "from-file"), ErrRoleStatic)
	assert.False(t, roleNames(t, store)["from-file"], "the config-file role was written into the new store")
}

// S3 servers watch the directory a filer-backed role store keeps roles in, so
// a store configured with its own basePath must report it, through the
// cache and the config-file overlay alike.
func TestRoleStoreDirectoryIsTheConfiguredBasePath(t *testing.T) {
	provider := func() string { return "localhost:8888" }
	cached, err := NewGenericCachedRoleStore(map[string]interface{}{"basePath": "/custom/roles"}, provider)
	require.NoError(t, err)
	mgr := startRoleServer(t, cached, "from-file")
	assert.Equal(t, "/custom/roles", mgr.RoleStoreDirectory())

	uncached, err := NewFilerRoleStore(nil, provider)
	require.NoError(t, err)
	mgr.SetRoleStore(uncached)
	assert.Equal(t, "/etc/iam/roles", mgr.RoleStoreDirectory())

	mgr.SetRoleStore(NewMemoryRoleStore())
	assert.Empty(t, mgr.RoleStoreDirectory(), "a memory store has no directory")
}
