package resources

import (
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// BuildCSIDriverResource constructs the CSIDriver cluster-scoped resource.
// Reference: csi/deploy/csi-driver.yaml
func BuildCSIDriverResource(cluster *blockv1alpha1.SeaweedBlockCluster) *storagev1.CSIDriver {
	attachRequired := false
	podInfoOnMount := false

	return &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name:   blockv1alpha1.CSIDriverName,
			Labels: ClusterScopedLabels(cluster, "csi-driver"),
		},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired: &attachRequired,
			PodInfoOnMount: &podInfoOnMount,
			VolumeLifecycleModes: []storagev1.VolumeLifecycleMode{
				storagev1.VolumeLifecyclePersistent,
			},
		},
	}
}
