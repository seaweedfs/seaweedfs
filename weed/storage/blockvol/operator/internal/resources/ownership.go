package resources

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// OwnershipStatus represents the result of checking cluster-scoped resource ownership.
type OwnershipStatus int

const (
	// OwnershipFree means the resource does not exist.
	OwnershipFree OwnershipStatus = iota
	// OwnershipOwned means the resource exists and is owned by this CR.
	OwnershipOwned
	// OwnershipConflict means the resource exists but is owned by a different CR.
	OwnershipConflict
	// OwnershipOrphan means the resource exists but has no ownership labels.
	OwnershipOrphan
)

// CheckOwnership determines if a cluster-scoped resource is owned by the given CR.
func CheckOwnership(obj metav1.Object, cluster *blockv1alpha1.SeaweedBlockCluster) OwnershipStatus {
	labels := obj.GetLabels()
	if labels == nil {
		return OwnershipOrphan
	}

	ns, hasNS := labels[blockv1alpha1.LabelOwnerNamespace]
	name, hasName := labels[blockv1alpha1.LabelOwnerName]

	if !hasNS && !hasName {
		return OwnershipOrphan
	}

	if ns == cluster.Namespace && name == cluster.Name {
		return OwnershipOwned
	}

	return OwnershipConflict
}

// ConflictOwner returns a human-readable "<namespace>/<name>" string identifying the
// conflicting owner of a cluster-scoped resource.
func ConflictOwner(obj metav1.Object) string {
	labels := obj.GetLabels()
	if labels == nil {
		return "unknown"
	}
	return fmt.Sprintf("%s/%s",
		labels[blockv1alpha1.LabelOwnerNamespace],
		labels[blockv1alpha1.LabelOwnerName],
	)
}

// IsOwnedBy returns true if the cluster-scoped resource's owner labels match the given CR.
func IsOwnedBy(obj metav1.Object, cluster *blockv1alpha1.SeaweedBlockCluster) bool {
	return CheckOwnership(obj, cluster) == OwnershipOwned
}
