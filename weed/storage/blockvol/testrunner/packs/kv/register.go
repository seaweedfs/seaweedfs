// Package kv is the SeaweedFS KV/object storage product pack for sw-test-runner.
// It registers actions for testing the standard SeaweedFS write/read/filer path.
package kv

import tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"

// RegisterPack registers all KV-specific actions on the registry.
func RegisterPack(r *tr.Registry) {
	r.RegisterFunc("kv_assign", tr.TierDevOps, kvAssign)
	r.RegisterFunc("kv_upload", tr.TierDevOps, kvUpload)
	r.RegisterFunc("kv_download", tr.TierDevOps, kvDownload)
	r.RegisterFunc("kv_verify", tr.TierDevOps, kvVerify)
	r.RegisterFunc("kv_delete", tr.TierDevOps, kvDelete)
	r.RegisterFunc("start_weed_filer", tr.TierDevOps, startWeedFiler)
	r.RegisterFunc("filer_put", tr.TierDevOps, filerPut)
	r.RegisterFunc("filer_get", tr.TierDevOps, filerGet)
	r.RegisterFunc("filer_delete", tr.TierDevOps, filerDelete)
}
