//go:build azurekms

package s3api

// Import the Azure KMS provider so its init() registers it with
// weed/kms. The provider is gated behind the `azurekms` build tag so that
// default builds do not pull in the Azure SDK.
import _ "github.com/seaweedfs/seaweedfs/weed/kms/azure"
