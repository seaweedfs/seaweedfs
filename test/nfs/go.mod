module seaweedfs-nfs-tests

go 1.25.0

// test/testutil lives inside the main seaweedfs module; pull it in via a
// local replace so this integration suite can reuse the shared port
// allocator and readiness helpers instead of reinventing them.
replace github.com/seaweedfs/seaweedfs => ../..

require (
	github.com/seaweedfs/seaweedfs v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.11.1
	github.com/willscott/go-nfs-client v0.0.0-20251022144359-801f10d98886
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
