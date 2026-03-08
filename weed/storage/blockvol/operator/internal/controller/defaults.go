package controller

import (
	"strings"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// applyDefaults fills zero-value fields with defaults.
// BUG-QA-6 fix: TrimSpace before comparing to catch whitespace-only strings.
func applyDefaults(spec *blockv1alpha1.SeaweedBlockClusterSpec) {
	spec.Image = strings.TrimSpace(spec.Image)
	if spec.Image == "" {
		spec.Image = blockv1alpha1.DefaultImage
	}
	spec.CSIImage = strings.TrimSpace(spec.CSIImage)
	if spec.CSIImage == "" {
		spec.CSIImage = blockv1alpha1.DefaultCSIImage
	}
	spec.StorageClassName = strings.TrimSpace(spec.StorageClassName)
	if spec.StorageClassName == "" {
		spec.StorageClassName = blockv1alpha1.DefaultStorageClassName
	}
	spec.CSINamespace = strings.TrimSpace(spec.CSINamespace)
	if spec.CSINamespace == "" {
		spec.CSINamespace = blockv1alpha1.DefaultCSINamespace
	}

	// CSI defaults
	if spec.CSI == nil {
		spec.CSI = &blockv1alpha1.CSISpec{}
	}
	if spec.CSI.ControllerReplicas == nil {
		r := int32(blockv1alpha1.DefaultControllerReplicas)
		spec.CSI.ControllerReplicas = &r
	}
	if spec.CSI.ProvisionerImage == "" {
		spec.CSI.ProvisionerImage = blockv1alpha1.DefaultProvisionerImage
	}
	if spec.CSI.RegistrarImage == "" {
		spec.CSI.RegistrarImage = blockv1alpha1.DefaultRegistrarImage
	}

	// Master defaults (full-stack mode)
	if spec.Master != nil {
		if spec.Master.Replicas == nil {
			r := int32(blockv1alpha1.DefaultMasterReplicas)
			spec.Master.Replicas = &r
		}
		if spec.Master.Port == 0 {
			spec.Master.Port = blockv1alpha1.DefaultMasterPort
		}
		if spec.Master.GRPCPort == 0 {
			spec.Master.GRPCPort = blockv1alpha1.DefaultMasterGRPCPort
		}
	}

	// Volume defaults (full-stack mode)
	if spec.Volume != nil {
		if spec.Volume.Replicas == nil {
			r := int32(blockv1alpha1.DefaultVolumeReplicas)
			spec.Volume.Replicas = &r
		}
		if spec.Volume.Port == 0 {
			spec.Volume.Port = blockv1alpha1.DefaultVolumePort
		}
		if spec.Volume.GRPCPort == 0 {
			spec.Volume.GRPCPort = blockv1alpha1.DefaultVolumeGRPCPort
		}
		spec.Volume.BlockDir = strings.TrimSpace(spec.Volume.BlockDir)
		if spec.Volume.BlockDir == "" {
			spec.Volume.BlockDir = blockv1alpha1.DefaultBlockDir
		}
		if spec.Volume.BlockListenPort == 0 {
			spec.Volume.BlockListenPort = blockv1alpha1.DefaultBlockListenPort
		}
	}
}
