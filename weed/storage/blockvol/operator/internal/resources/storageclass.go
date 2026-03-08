package resources

import (
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// BuildStorageClass constructs the StorageClass for block volumes.
// Reference: csi/deploy/storageclass.yaml
func BuildStorageClass(cluster *blockv1alpha1.SeaweedBlockCluster) *storagev1.StorageClass {
	scName := cluster.Spec.StorageClassName
	if scName == "" {
		scName = blockv1alpha1.DefaultStorageClassName
	}

	bindingMode := storagev1.VolumeBindingWaitForFirstConsumer
	reclaimPolicy := corev1.PersistentVolumeReclaimDelete

	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   scName,
			Labels: ClusterScopedLabels(cluster, "storageclass"),
		},
		Provisioner:       blockv1alpha1.CSIDriverName,
		VolumeBindingMode: &bindingMode,
		ReclaimPolicy:     &reclaimPolicy,
	}
}
