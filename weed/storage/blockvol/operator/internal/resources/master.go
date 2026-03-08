package resources

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// MasterServiceName returns the headless Service name for the master.
func MasterServiceName(cluster *blockv1alpha1.SeaweedBlockCluster) string {
	return fmt.Sprintf("%s-master", cluster.Name)
}

// BuildMasterService constructs the headless Service for master StatefulSet.
func BuildMasterService(cluster *blockv1alpha1.SeaweedBlockCluster) *corev1.Service {
	port := cluster.Spec.Master.Port
	if port == 0 {
		port = blockv1alpha1.DefaultMasterPort
	}
	grpcPort := cluster.Spec.Master.GRPCPort
	if grpcPort == 0 {
		grpcPort = blockv1alpha1.DefaultMasterGRPCPort
	}

	name := MasterServiceName(cluster)
	labels := ComponentLabels(cluster, "master")
	selectorLabels := SelectorLabels(cluster, "master")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			Selector:                 selectorLabels,
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: port, TargetPort: intstr.FromInt32(port)},
				{Name: "grpc", Port: grpcPort, TargetPort: intstr.FromInt32(grpcPort)},
			},
		},
	}

	SetOwnerReference(cluster, &svc.ObjectMeta)
	return svc
}

// BuildMasterStatefulSet constructs the master StatefulSet.
func BuildMasterStatefulSet(cluster *blockv1alpha1.SeaweedBlockCluster) *appsv1.StatefulSet {
	ms := cluster.Spec.Master
	replicas := int32(1)
	if ms.Replicas != nil {
		replicas = *ms.Replicas
	}
	port := ms.Port
	if port == 0 {
		port = blockv1alpha1.DefaultMasterPort
	}

	name := MasterServiceName(cluster)
	labels := ComponentLabels(cluster, "master")
	selectorLabels := SelectorLabels(cluster, "master")

	// Build peers string for single-replica: "<name>-master-0.<name>-master.<ns>:<port>"
	peers := fmt.Sprintf("%s-0.%s.%s:%d", name, name, cluster.Namespace, port)

	args := []string{
		"master",
		fmt.Sprintf("-port=%d", port),
		"-mdir=/data",
		"-ip.bind=0.0.0.0",
		fmt.Sprintf("-peers=%s", peers),
	}
	args = append(args, ms.ExtraArgs...)

	image := cluster.Spec.Image
	if image == "" {
		image = blockv1alpha1.DefaultImage
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "master",
							Image:           image,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Command:         []string{"/usr/bin/weed"},
							Args:            args,
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: port},
								{Name: "grpc", ContainerPort: ms.GRPCPort},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/cluster/status",
										Port: intstr.FromInt32(port),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       15,
							},
							Resources: ms.Resources,
						},
					},
				},
			},
		},
	}

	// VolumeClaimTemplate for data
	if ms.Storage != nil && ms.Storage.Size != "" {
		pvcSpec := corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(ms.Storage.Size), // safe: validated by controller
				},
			},
		}
		// M3 fix: wire StorageClassName into PVC
		if ms.Storage.StorageClassName != nil {
			pvcSpec.StorageClassName = ms.Storage.StorageClassName
		}
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec:       pvcSpec,
			},
		}
	} else {
		// Use emptyDir if no storage specified
		sts.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
	}

	SetOwnerReference(cluster, &sts.ObjectMeta)
	return sts
}
