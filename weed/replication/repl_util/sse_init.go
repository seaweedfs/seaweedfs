package repl_util

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	sseInitMu          sync.Mutex
	sseInitialized     bool
)

// InitializeSSEForReplication sets up SSE-S3 and SSE-KMS decryption so that
// replication sinks can transparently decrypt encrypted objects.
// SSE-S3 is initialized from the filer (KEK stored on filer).
// SSE-KMS is initialized from Viper config (security.toml [kms] section or
// WEED_KMS_* environment variables).
// SSE-C cannot be decrypted (customer key not available) and will error at
// decryption time.
//
// Safe to call multiple times; only the first successful initialization takes
// effect. Failed attempts do not prevent future retries.
func InitializeSSEForReplication(filerSource filer_pb.FilerClient) error {
	sseInitMu.Lock()
	defer sseInitMu.Unlock()
	if sseInitialized {
		return nil
	}

	// Initialize SSE-S3 key manager from filer
	if err := s3api.GetSSES3KeyManager().InitializeWithFiler(filerSource); err != nil {
		return err
	}

	// Attempt KMS initialization from Viper config.
	// KMS configuration is typically in the S3 config file which the
	// replication commands don't load directly. Support loading from
	// security.toml [kms] section or WEED_KMS_* environment variables.
	loader := kms.NewConfigLoader(util.GetViper())
	if err := loader.LoadConfigurations(); err != nil {
		glog.Warningf("KMS initialization from config failed: %v (SSE-KMS decryption will not be available)", err)
	} else if err := loader.ValidateConfiguration(); err != nil {
		glog.Warningf("KMS configuration validation failed: %v (SSE-KMS decryption will not be available)", err)
	} else {
		glog.V(0).Infof("KMS initialized for replication")
	}

	sseInitialized = true
	return nil
}
