package resources

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

const (
	labelApp       = "app.kubernetes.io/name"
	labelInstance   = "app.kubernetes.io/instance"
	labelComponent = "app.kubernetes.io/component"
	labelManagedBy = "app.kubernetes.io/managed-by"

	managerName = "sw-block-operator"
)

// CommonLabels returns the base label set for all operator-managed resources.
func CommonLabels(cluster *blockv1alpha1.SeaweedBlockCluster) map[string]string {
	return map[string]string{
		labelApp:       "sw-block",
		labelInstance:   cluster.Name,
		labelManagedBy: managerName,
	}
}

// ComponentLabels returns labels for a specific component (e.g. "csi-controller", "master").
func ComponentLabels(cluster *blockv1alpha1.SeaweedBlockCluster, component string) map[string]string {
	labels := CommonLabels(cluster)
	labels[labelComponent] = component
	return labels
}

// SelectorLabels returns the minimal label set used in matchLabels.
func SelectorLabels(cluster *blockv1alpha1.SeaweedBlockCluster, component string) map[string]string {
	return map[string]string{
		labelApp:       "sw-block",
		labelInstance:   cluster.Name,
		labelComponent: component,
	}
}

// OwnerLabels returns labels that identify the owning CR for cluster-scoped resources.
func OwnerLabels(cluster *blockv1alpha1.SeaweedBlockCluster) map[string]string {
	return map[string]string{
		blockv1alpha1.LabelOwnerNamespace: cluster.Namespace,
		blockv1alpha1.LabelOwnerName:      cluster.Name,
	}
}

// ClusterScopedLabels returns the full label set for cluster-scoped resources
// (or cross-namespace resources that cannot use ownerReference).
func ClusterScopedLabels(cluster *blockv1alpha1.SeaweedBlockCluster, component string) map[string]string {
	labels := ComponentLabels(cluster, component)
	for k, v := range OwnerLabels(cluster) {
		labels[k] = v
	}
	return labels
}

// SetOwnerReference sets the ownerReference for same-namespace resources only.
// Cross-namespace resources must use ClusterScopedLabels + finalizer cleanup instead,
// because Kubernetes GC ignores cross-namespace ownerReferences.
func SetOwnerReference(cluster *blockv1alpha1.SeaweedBlockCluster, obj metav1.Object) {
	t := true
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion:         blockv1alpha1.GroupVersion.String(),
			Kind:               "SeaweedBlockCluster",
			Name:               cluster.Name,
			UID:                cluster.UID,
			Controller:         &t,
			BlockOwnerDeletion: &t,
		},
	})
}

// SetCrossNamespaceOwnership sets ownership labels (not ownerRef) on resources that
// live in a different namespace than the CR. These are cleaned up by the finalizer.
func SetCrossNamespaceOwnership(cluster *blockv1alpha1.SeaweedBlockCluster, obj metav1.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[blockv1alpha1.LabelOwnerNamespace] = cluster.Namespace
	labels[blockv1alpha1.LabelOwnerName] = cluster.Name
	obj.SetLabels(labels)
}
