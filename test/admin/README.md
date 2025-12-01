# Admin Integration Tests

This directory contains integration tests for the SeaweedFS admin server functionality.

## Tests

### user_creation_integration_test.go

Integration tests for user creation and credential management, specifically testing:

- **Issue #7575**: Fixed bug where user creation failed with "filer address function not configured" error
- Verification that the `FilerEtcStore` correctly implements the `SetFilerAddressFunc` interface
- Admin server setup process with filer_etc credential store

## Running Tests

Run all integration tests:
```bash
go test -v ./test/admin
```

Run specific tests:
```bash
go test -v ./test/admin -run TestUserCreationWithFilerEtcStore
```

Skip integration tests (short mode):
```bash
go test -short ./test/admin
```

## Test Requirements

These tests do not require a running filer instance. They verify that:
1. The credential store interface is correctly implemented
2. The filer address function can be properly configured
3. Errors are appropriate (connection errors, not configuration errors)

## Related Files

- `weed/admin/dash/admin_server.go` - Admin server initialization with credential manager setup
- `weed/credential/filer_etc/filer_etc_store.go` - FilerEtcStore implementation
- `weed/admin/dash/user_management.go` - User management operations
- `weed/admin/dash/user_management_test.go` - Unit tests for user management

