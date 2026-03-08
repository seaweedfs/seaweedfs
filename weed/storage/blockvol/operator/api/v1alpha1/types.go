// Package v1alpha1 contains API types for the SeaweedFS Block operator.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// CSIDriverName is the well-known CSI driver registration name.
	CSIDriverName = "block.csi.seaweedfs.com"

	// FinalizerName is the finalizer used for cluster-scoped resource cleanup.
	FinalizerName = "block.seaweedfs.com/finalizer"

	// AnnotationRotateSecret triggers CHAP secret regeneration when set to an RFC 3339 timestamp.
	AnnotationRotateSecret = "block.seaweedfs.com/rotate-secret"

	// LabelOwnerNamespace identifies the owning CR namespace on cluster-scoped resources.
	LabelOwnerNamespace = "block.seaweedfs.com/owner-namespace"
	// LabelOwnerName identifies the owning CR name on cluster-scoped resources.
	LabelOwnerName = "block.seaweedfs.com/owner-name"

	// AnnotationLastRotated records the timestamp of the last secret rotation.
	AnnotationLastRotated = "block.seaweedfs.com/last-rotated"

	// Default values.
	DefaultImage               = "chrislusf/seaweedfs:latest"
	DefaultCSIImage            = "sw-block-csi:local"
	DefaultMasterPort          = 9333
	DefaultMasterGRPCPort      = 19333
	DefaultVolumePort          = 8080
	DefaultVolumeGRPCPort      = 18080
	DefaultBlockListenPort     = 3260
	DefaultBlockDir            = "/data1/block"
	DefaultStorageClassName    = "sw-block"
	DefaultCSINamespace        = "kube-system"
	DefaultProvisionerImage    = "registry.k8s.io/sig-storage/csi-provisioner:v5.1.0"
	DefaultRegistrarImage      = "registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.12.0"
	DefaultControllerReplicas  = 1
	DefaultMasterReplicas      = 1
	DefaultVolumeReplicas      = 1
	DefaultImagePullPolicy     = corev1.PullIfNotPresent
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=sbc
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.masterAddress`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SeaweedBlockCluster is the Schema for the seaweedBlockClusters API.
type SeaweedBlockCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SeaweedBlockClusterSpec   `json:"spec,omitempty"`
	Status SeaweedBlockClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SeaweedBlockClusterList contains a list of SeaweedBlockCluster.
type SeaweedBlockClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SeaweedBlockCluster `json:"items"`
}

// SeaweedBlockClusterSpec defines the desired state of SeaweedBlockCluster.
type SeaweedBlockClusterSpec struct {
	// Image is the SeaweedFS image for master + volume servers.
	// +optional
	Image string `json:"image,omitempty"`
	// CSIImage is the block CSI driver image.
	// +optional
	CSIImage string `json:"csiImage,omitempty"`

	// Master configures a full-stack SeaweedFS master deployment.
	// Mutually exclusive with MasterRef.
	// +optional
	Master *MasterSpec `json:"master,omitempty"`
	// Volume configures a full-stack SeaweedFS volume deployment.
	// Requires Master to be set (full-stack mode).
	// +optional
	Volume *VolumeSpec `json:"volume,omitempty"`
	// MasterRef points to an existing external SeaweedFS master.
	// Mutually exclusive with Master.
	// +optional
	MasterRef *MasterRef `json:"masterRef,omitempty"`

	// CSI configures the CSI driver components.
	// +optional
	CSI *CSISpec `json:"csi,omitempty"`
	// Auth configures CHAP authentication.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// StorageClassName is the name of the StorageClass to create.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
	// CSINamespace is the namespace for CSI components (controller, node, SA).
	// +optional
	CSINamespace string `json:"csiNamespace,omitempty"`

	// AdoptExistingStorageClass allows the operator to adopt a pre-existing
	// StorageClass that was created outside this operator.
	// +optional
	AdoptExistingStorageClass bool `json:"adoptExistingStorageClass,omitempty"`
}

// MasterSpec configures the SeaweedFS master StatefulSet (full-stack mode).
type MasterSpec struct {
	// Replicas is the number of master replicas. Only 1 is supported in 9A.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Port is the HTTP port for the master.
	// +optional
	Port int32 `json:"port,omitempty"`
	// GRPCPort is the gRPC port for the master.
	// +optional
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// Storage configures the PVC for /data.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`
	// Resources defines compute resources for the master container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// ExtraArgs are additional command-line arguments.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// VolumeSpec configures the SeaweedFS volume StatefulSet (full-stack mode).
type VolumeSpec struct {
	// Replicas is the number of volume server replicas.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Port is the HTTP port.
	// +optional
	Port int32 `json:"port,omitempty"`
	// GRPCPort is the gRPC port.
	// +optional
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// BlockDir is the directory for .blk block volume files.
	// +optional
	BlockDir string `json:"blockDir,omitempty"`
	// BlockListenPort is the iSCSI target listen port.
	// +optional
	BlockListenPort int32 `json:"blockListenPort,omitempty"`
	// PortalOverride overrides the auto-derived iSCSI portal address.
	// +optional
	PortalOverride string `json:"portalOverride,omitempty"`
	// Storage configures the PVC for /data1.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`
	// Resources defines compute resources for the volume container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// ExtraArgs are additional command-line arguments.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// StorageSpec defines PVC storage configuration.
type StorageSpec struct {
	// Size is the requested storage size (e.g. "5Gi").
	Size string `json:"size,omitempty"`
	// StorageClassName is the storage class for the PVC.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// MasterRef points to an existing SeaweedFS master (CSI-only mode).
type MasterRef struct {
	// Address is the master address (e.g. "seaweedfs-master.default:9333").
	Address string `json:"address"`
}

// CSISpec configures CSI driver components.
type CSISpec struct {
	// ControllerReplicas is the number of CSI controller replicas.
	// +optional
	ControllerReplicas *int32 `json:"controllerReplicas,omitempty"`
	// ProvisionerImage is the CSI provisioner sidecar image.
	// +optional
	ProvisionerImage string `json:"provisionerImage,omitempty"`
	// RegistrarImage is the CSI node-driver-registrar sidecar image.
	// +optional
	RegistrarImage string `json:"registrarImage,omitempty"`
}

// AuthSpec configures CHAP authentication.
type AuthSpec struct {
	// SecretRef references a pre-existing CHAP secret.
	// If nil, the operator auto-generates one.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// SeaweedBlockClusterStatus defines the observed state of SeaweedBlockCluster.
type SeaweedBlockClusterStatus struct {
	// Phase is the high-level cluster state.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`
	// MasterAddress is the resolved master address.
	// +optional
	MasterAddress string `json:"masterAddress,omitempty"`
	// Conditions represent the latest observations of the cluster's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterPhase represents the lifecycle phase of the cluster.
// +kubebuilder:validation:Enum=Pending;Running;Failed
type ClusterPhase string

const (
	PhasePending ClusterPhase = "Pending"
	PhaseRunning ClusterPhase = "Running"
	PhaseFailed  ClusterPhase = "Failed"
)

// Condition types.
const (
	ConditionMasterReady      = "MasterReady"
	ConditionVolumeReady      = "VolumeReady"
	ConditionCSIReady         = "CSIReady"
	ConditionAuthConfigured   = "AuthConfigured"
	ConditionResourceConflict = "ResourceConflict"
	ConditionValidationFailed = "ValidationFailed"
)
