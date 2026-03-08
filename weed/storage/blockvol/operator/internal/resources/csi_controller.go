package resources

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// CSIControllerName returns the Deployment name for the CSI controller.
func CSIControllerName(cluster *blockv1alpha1.SeaweedBlockCluster) string {
	return fmt.Sprintf("%s-csi-controller", cluster.Name)
}

// BuildCSIControllerDeployment constructs the CSI controller Deployment.
// Reference: csi/deploy/csi-controller.yaml
func BuildCSIControllerDeployment(cluster *blockv1alpha1.SeaweedBlockCluster, masterAddr, csiNS string) *appsv1.Deployment {
	replicas := int32(blockv1alpha1.DefaultControllerReplicas)
	if cluster.Spec.CSI != nil && cluster.Spec.CSI.ControllerReplicas != nil {
		replicas = *cluster.Spec.CSI.ControllerReplicas
	}

	csiImage := cluster.Spec.CSIImage
	if csiImage == "" {
		csiImage = blockv1alpha1.DefaultCSIImage
	}

	provisionerImage := blockv1alpha1.DefaultProvisionerImage
	if cluster.Spec.CSI != nil && cluster.Spec.CSI.ProvisionerImage != "" {
		provisionerImage = cluster.Spec.CSI.ProvisionerImage
	}

	saName := ServiceAccountName()
	name := CSIControllerName(cluster)
	labels := ComponentLabels(cluster, "csi-controller")
	selectorLabels := SelectorLabels(cluster, "csi-controller")

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: csiNS,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: saName,
					Containers: []corev1.Container{
						{
							Name:            "block-csi",
							Image:           csiImage,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Args: []string{
								"-endpoint=unix:///csi/csi.sock",
								"-mode=controller",
								fmt.Sprintf("-master=%s", masterAddr),
								"-node-id=$(NODE_NAME)",
							},
							Env: []corev1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "socket-dir", MountPath: "/csi"},
							},
						},
						{
							Name:            "csi-provisioner",
							Image:           provisionerImage,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Args: []string{
								"--csi-address=/csi/csi.sock",
								"--feature-gates=Topology=true",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "socket-dir", MountPath: "/csi"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "socket-dir",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	// CSI resources live in csiNamespace (typically kube-system), which differs from
	// the CR namespace. Cross-namespace ownerReferences are ignored by k8s GC, so we
	// use ownership labels + finalizer cleanup instead.
	SetCrossNamespaceOwnership(cluster, &dep.ObjectMeta)
	return dep
}
