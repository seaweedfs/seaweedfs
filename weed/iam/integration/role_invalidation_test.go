package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockFilerRoleStore partially mocks FilerRoleStore for testing
// In a real scenario, we might want a cleaner interface-based mock,
// but for now we'll focus on testing the Caching layer logic.
type MockRoleStore struct {
	mock.Mock
}

func (m *MockRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	args := m.Called(ctx, filerAddress, roleName, role)
	return args.Error(0)
}

func (m *MockRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	args := m.Called(ctx, filerAddress, roleName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*RoleDefinition), args.Error(1)
}

func (m *MockRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	args := m.Called(ctx, filerAddress)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	args := m.Called(ctx, filerAddress, roleName)
	return args.Error(0)
}

func (m *MockRoleStore) ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error) {
	args := m.Called(ctx, filerAddress)
	return args.Get(0).([]*RoleDefinition), args.Error(1)
}

func (m *MockRoleStore) InvalidateCache(roleName string) {
	m.Called(roleName)
}

func TestRoleStoreInvalidation(t *testing.T) {
	// Create a generic cached role store with a mock underlying store?
	// Since CachedFilerRoleStore takes *FilerRoleStore struct (not interface) in the current code (based on my read),
	// it might be hard to mock the internal FilerRoleStore perfectly without refactoring.
	// However, looking at role_store.go, NewCachedFilerRoleStore returns *CachedFilerRoleStore
	// which has a `filerStore *FilerRoleStore`.
	// Wait, the interface RoleStore is implemented by CachedFilerRoleStore.

	// Let's test the interface behavior using the MemoryRoleStore as it's easier to verify behaviorally
	// or we can assume the cache logic works if we can't easily mock the FilerRoleStore struct.
	// But `CachedFilerRoleStore` logic IS what we want to test.
	// `CachedFilerRoleStore` uses `ccache`.

	// We can use a trick: standard integration tests might need a real filer,
	// but unit tests should avoid external deps.
	// For this unit test, let's verify that the InvalidateCache method effectively removes items from the cache.

	// Since we can't easily mock FilerRoleStore (it's a struct), we'll skip mocking the inner store
	// and instead rely on the fact that we can manipulate the cache directly or use the available methods
	// if we can construct it safely.
	// But `NewCachedFilerRoleStore` tries to init a FilerRoleStore which might fail if we don't pass valid config?
	// No, `NewFilerRoleStore` just sets paths. It doesn't connect until we call methods.
	// So we can instantiate it!

	t.Run("CachedFilerRoleStore Invalidation", func(t *testing.T) {
		// Initialize the store
		store, err := NewCachedFilerRoleStore(nil, nil, nil)
		assert.NoError(t, err)

		// Manually inject a role into the cache (using internal knowledge or via a successful fetch if we could mock the backend)
		// Since we can't easily mock the backend response without a real filer client,
		// we might need to inspect the cache or assume InvalidateCache does its job.
		// Actually, `CachedFilerRoleStore` has `cache *ccache.Cache`.
		// We can't access private fields outside the package.
		// BUT, we are in `package integration`! So we CAN access private fields of other types in the same package!
		// Wait, `role_invalidation_test.go` is in `package integration`.
		// `CachedFilerRoleStore` is in `package integration`.
		// So we have access to `cache`.

		roleName := "TestRole"
		roleDef := &RoleDefinition{RoleName: roleName}

		// Inject into cache
		store.cache.Set(roleName, roleDef, 10*time.Minute)

		// Verify it's there
		item := store.cache.Get(roleName)
		assert.NotNil(t, item)

		// Invalidate
		store.InvalidateCache(roleName)

		// Verify it's gone
		item = store.cache.Get(roleName)
		assert.Nil(t, item)
	})

	t.Run("MemoryRoleStore Invalidation (No-op check)", func(t *testing.T) {
		store := NewMemoryRoleStore()
		roleName := "TestRole"
		store.roles[roleName] = &RoleDefinition{RoleName: roleName}

		// Invalidate (should do nothing as memory store is source of truth)
		store.InvalidateCache(roleName)

		// Verify still there
		_, err := store.GetRole(context.Background(), "", roleName)
		assert.NoError(t, err)
	})
}

func TestIAMManagerInvalidation(t *testing.T) {
	// Setup IAMManager
	mgr := NewIAMManager()
	
	// Use a mock RoleStore
	mockStore := new(MockRoleStore)
	mgr.roleStore = mockStore
	mgr.initialized = true

	roleName := "UpdatedRole"

	// Expect InvalidateCache to be called
	mockStore.On("InvalidateCache", roleName).Return()

	// Call InvalidateRoleCache
	mgr.InvalidateRoleCache(roleName)

	// Verify expectation
	mockStore.AssertExpectations(t)
}

// Mock Policy implementations for context
// Note: We don't need full policy engine for these invalidation tests
