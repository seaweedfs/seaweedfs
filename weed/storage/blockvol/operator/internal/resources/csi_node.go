package resources

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// CSINodeName returns the DaemonSet name for the CSI node plugin.
func CSINodeName(cluster *blockv1alpha1.SeaweedBlockCluster) string {
	return fmt.Sprintf("%s-csi-node", cluster.Name)
}

// BuildCSINodeDaemonSet constructs the CSI node DaemonSet.
// Reference: csi/deploy/csi-node.yaml
func BuildCSINodeDaemonSet(cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) *appsv1.DaemonSet {
	csiImage := cluster.Spec.CSIImage
	if csiImage == "" {
		csiImage = blockv1alpha1.DefaultCSIImage
	}

	registrarImage := blockv1alpha1.DefaultRegistrarImage
	if cluster.Spec.CSI != nil && cluster.Spec.CSI.RegistrarImage != "" {
		registrarImage = cluster.Spec.CSI.RegistrarImage
	}

	saName := ServiceAccountName()
	name := CSINodeName(cluster)
	labels := ComponentLabels(cluster, "csi-node")
	selectorLabels := SelectorLabels(cluster, "csi-node")

	hostPathDir := corev1.HostPathDirectory
	hostPathDirOrCreate := corev1.HostPathDirectoryOrCreate
	privileged := true
	bidirectional := corev1.MountPropagationBidirectional
	hostNetwork := true
	hostPID := true

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: csiNS,
			Labels:    labels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: saName,
					HostNetwork:        hostNetwork,
					HostPID:            hostPID,
					Containers: []corev1.Container{
						{
							Name:            "block-csi",
							Image:           csiImage,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Args: []string{
								"-endpoint=unix:///csi/csi.sock",
								"-mode=node",
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
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "socket-dir", MountPath: "/csi"},
								{Name: "kubelet-dir", MountPath: "/var/lib/kubelet", MountPropagation: &bidirectional},
								{Name: "dev", MountPath: "/dev"},
								{Name: "iscsi-dir", MountPath: "/etc/iscsi"},
							},
						},
						{
							Name:            "csi-node-driver-registrar",
							Image:           registrarImage,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Args: []string{
								"--csi-address=/csi/csi.sock",
								fmt.Sprintf("--kubelet-registration-path=/var/lib/kubelet/plugins/%s/csi.sock", blockv1alpha1.CSIDriverName),
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "socket-dir", MountPath: "/csi"},
								{Name: "registration-dir", MountPath: "/registration"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "socket-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: fmt.Sprintf("/var/lib/kubelet/plugins/%s", blockv1alpha1.CSIDriverName),
									Type: &hostPathDirOrCreate,
								},
							},
						},
						{
							Name: "kubelet-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet",
									Type: &hostPathDir,
								},
							},
						},
						{
							Name: "dev",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/dev",
									Type: &hostPathDir,
								},
							},
						},
						{
							Name: "iscsi-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/etc/iscsi",
									Type: &hostPathDirOrCreate,
								},
							},
						},
						{
							Name: "registration-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/plugins_registry",
									Type: &hostPathDir,
								},
							},
						},
					},
				},
			},
		},
	}

	// Cross-namespace: use labels + finalizer, not ownerRef.
	SetCrossNamespaceOwnership(cluster, &ds.ObjectMeta)
	return ds
}
