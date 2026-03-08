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

// VolumeServiceName returns the headless Service name for volume servers.
func VolumeServiceName(cluster *blockv1alpha1.SeaweedBlockCluster) string {
	return fmt.Sprintf("%s-volume", cluster.Name)
}

// BuildVolumeService constructs the headless Service for volume StatefulSet.
func BuildVolumeService(cluster *blockv1alpha1.SeaweedBlockCluster) *corev1.Service {
	vs := cluster.Spec.Volume
	port := vs.Port
	if port == 0 {
		port = blockv1alpha1.DefaultVolumePort
	}
	grpcPort := vs.GRPCPort
	if grpcPort == 0 {
		grpcPort = blockv1alpha1.DefaultVolumeGRPCPort
	}
	blockPort := vs.BlockListenPort
	if blockPort == 0 {
		blockPort = blockv1alpha1.DefaultBlockListenPort
	}

	name := VolumeServiceName(cluster)
	labels := ComponentLabels(cluster, "volume")
	selectorLabels := SelectorLabels(cluster, "volume")

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
				{Name: "iscsi", Port: blockPort, TargetPort: intstr.FromInt32(blockPort)},
			},
		},
	}

	SetOwnerReference(cluster, &svc.ObjectMeta)
	return svc
}

// BuildVolumeStatefulSet constructs the volume server StatefulSet.
func BuildVolumeStatefulSet(cluster *blockv1alpha1.SeaweedBlockCluster, masterAddr string) *appsv1.StatefulSet {
	vs := cluster.Spec.Volume
	replicas := int32(1)
	if vs.Replicas != nil {
		replicas = *vs.Replicas
	}
	port := vs.Port
	if port == 0 {
		port = blockv1alpha1.DefaultVolumePort
	}
	grpcPort := vs.GRPCPort
	if grpcPort == 0 {
		grpcPort = blockv1alpha1.DefaultVolumeGRPCPort
	}
	blockDir := vs.BlockDir
	if blockDir == "" {
		blockDir = blockv1alpha1.DefaultBlockDir
	}
	blockPort := vs.BlockListenPort
	if blockPort == 0 {
		blockPort = blockv1alpha1.DefaultBlockListenPort
	}

	name := VolumeServiceName(cluster)
	labels := ComponentLabels(cluster, "volume")
	selectorLabels := SelectorLabels(cluster, "volume")

	image := cluster.Spec.Image
	if image == "" {
		image = blockv1alpha1.DefaultImage
	}

	args := []string{
		"volume",
		fmt.Sprintf("-port=%d", port),
		"-dir=/data1",
		fmt.Sprintf("-master=%s", masterAddr),
		"-ip.bind=0.0.0.0",
		fmt.Sprintf("-block.dir=%s", blockDir),
		fmt.Sprintf("-block.listen=0.0.0.0:%d", blockPort),
	}

	// Portal override or auto-derive via downward API
	if vs.PortalOverride != "" {
		args = append(args, fmt.Sprintf("-block.portal=%s", vs.PortalOverride))
	} else {
		args = append(args, fmt.Sprintf("-block.portal=$(POD_IP):%d,1", blockPort))
	}

	args = append(args, vs.ExtraArgs...)

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
							Name:            "volume",
							Image:           image,
							ImagePullPolicy: blockv1alpha1.DefaultImagePullPolicy,
							Command:         []string{"/usr/bin/weed"},
							Args:            args,
							Env: []corev1.EnvVar{
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: port},
								{Name: "grpc", ContainerPort: grpcPort},
								{Name: "iscsi", ContainerPort: blockPort},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data1"},
							},
							// M1 fix: readiness probe for volume server
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/status",
										Port: intstr.FromInt32(port),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       15,
							},
							Resources: vs.Resources,
						},
					},
				},
			},
		},
	}

	// VolumeClaimTemplate for data
	if vs.Storage != nil && vs.Storage.Size != "" {
		pvcSpec := corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(vs.Storage.Size), // safe: validated by controller
				},
			},
		}
		// M3 fix: wire StorageClassName into PVC
		if vs.Storage.StorageClassName != nil {
			pvcSpec.StorageClassName = vs.Storage.StorageClassName
		}
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec:       pvcSpec,
			},
		}
	} else {
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
