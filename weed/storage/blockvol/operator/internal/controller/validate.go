package controller

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// maxDerivedNameSuffix is the longest suffix appended to the CR name when
// deriving sub-resource names. Currently "-csi-controller" (16 chars).
const maxDerivedNameSuffix = 16

// maxCRNameLength = 63 (DNS label limit) - maxDerivedNameSuffix.
const maxCRNameLength = 63 - maxDerivedNameSuffix

// dns1123LabelRegex matches RFC 1123 DNS labels (lowercase alphanumeric + hyphens,
// must start and end with alphanumeric).
var dns1123LabelRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// reservedMasterFlags are flag prefixes that the operator sets on the master
// container and must not be overridden via ExtraArgs.
var reservedMasterFlags = []string{
	"-port=", "-mdir=", "-ip.bind=", "-peers=",
}

// reservedVolumeFlags are flag prefixes that the operator sets on the volume
// container and must not be overridden via ExtraArgs.
var reservedVolumeFlags = []string{
	"-port=", "-dir=", "-master=", "-ip.bind=",
	"-block.dir=", "-block.listen=", "-block.portal=",
}

// validateName checks the CR name for DNS compatibility and derived name length.
// BUG-QA-3 fix: reject names whose derived sub-resource names exceed 63 chars.
// BUG-QA-5 fix: reject names that aren't valid RFC 1123 DNS labels.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("metadata.name must not be empty")
	}
	if !dns1123LabelRegex.MatchString(name) {
		return fmt.Errorf("metadata.name %q is not a valid DNS label (must match [a-z0-9]([a-z0-9-]*[a-z0-9])?)", name)
	}
	if len(name) > maxCRNameLength {
		return fmt.Errorf("metadata.name %q is too long (%d chars); max is %d to keep derived resource names within the 63-char DNS label limit",
			name, len(name), maxCRNameLength)
	}
	return nil
}

// validate checks the spec for invalid field combinations and values.
func validate(spec *blockv1alpha1.SeaweedBlockClusterSpec) error {
	if spec.Master != nil && spec.MasterRef != nil {
		return fmt.Errorf("spec.master and spec.masterRef are mutually exclusive")
	}
	if spec.Master == nil && spec.MasterRef == nil {
		return fmt.Errorf("one of spec.master or spec.masterRef is required")
	}
	if spec.Volume != nil && spec.Master == nil {
		return fmt.Errorf("spec.volume requires spec.master (full-stack mode)")
	}

	// BUG-QA-4 fix: separate error messages for replicas=0 vs replicas>1
	if spec.Master != nil && spec.Master.Replicas != nil {
		r := *spec.Master.Replicas
		if r < 1 {
			return fmt.Errorf("spec.master.replicas must be at least 1")
		}
		if r > 1 {
			return fmt.Errorf("master HA (replicas > 1) deferred to Phase 9C")
		}
	}

	if spec.MasterRef != nil && spec.MasterRef.Address == "" {
		return fmt.Errorf("spec.masterRef.address must not be empty")
	}

	// H2 fix: validate storage size before builders attempt to parse it
	if err := validateStorageSize("spec.master.storage.size", spec.Master != nil && spec.Master.Storage != nil, func() string {
		if spec.Master != nil && spec.Master.Storage != nil {
			return spec.Master.Storage.Size
		}
		return ""
	}); err != nil {
		return err
	}
	if err := validateStorageSize("spec.volume.storage.size", spec.Volume != nil && spec.Volume.Storage != nil, func() string {
		if spec.Volume != nil && spec.Volume.Storage != nil {
			return spec.Volume.Storage.Size
		}
		return ""
	}); err != nil {
		return err
	}

	// L3 fix: port range validation
	if err := validatePort("spec.master.port", spec.Master, func(m *blockv1alpha1.MasterSpec) int32 { return m.Port }); err != nil {
		return err
	}
	if err := validatePort("spec.master.grpcPort", spec.Master, func(m *blockv1alpha1.MasterSpec) int32 { return m.GRPCPort }); err != nil {
		return err
	}
	if spec.Volume != nil {
		if spec.Volume.Port != 0 && (spec.Volume.Port < 1 || spec.Volume.Port > 65535) {
			return fmt.Errorf("spec.volume.port %d is out of range (1-65535)", spec.Volume.Port)
		}
		if spec.Volume.GRPCPort != 0 && (spec.Volume.GRPCPort < 1 || spec.Volume.GRPCPort > 65535) {
			return fmt.Errorf("spec.volume.grpcPort %d is out of range (1-65535)", spec.Volume.GRPCPort)
		}
		if spec.Volume.BlockListenPort != 0 && (spec.Volume.BlockListenPort < 1 || spec.Volume.BlockListenPort > 65535) {
			return fmt.Errorf("spec.volume.blockListenPort %d is out of range (1-65535)", spec.Volume.BlockListenPort)
		}
	}

	// BUG-QA-1 fix: reject ExtraArgs that override operator-managed flags
	if spec.Master != nil {
		if err := validateExtraArgs("spec.master.extraArgs", spec.Master.ExtraArgs, reservedMasterFlags); err != nil {
			return err
		}
	}
	if spec.Volume != nil {
		if err := validateExtraArgs("spec.volume.extraArgs", spec.Volume.ExtraArgs, reservedVolumeFlags); err != nil {
			return err
		}
	}

	return nil
}

// validateStorageSize validates a storage size string is parseable and positive.
// BUG-QA-7 fix: reject zero/negative sizes.
func validateStorageSize(fieldName string, hasStorage bool, getSize func() string) error {
	if !hasStorage {
		return nil
	}
	size := getSize()
	if size == "" {
		return nil
	}
	qty, err := resource.ParseQuantity(size)
	if err != nil {
		return fmt.Errorf("%s %q is invalid: %w", fieldName, size, err)
	}
	if qty.Cmp(resource.MustParse("1")) < 0 {
		return fmt.Errorf("%s %q must be a positive value (e.g. \"1Gi\")", fieldName, size)
	}
	return nil
}

func validatePort[T any](name string, obj *T, getPort func(*T) int32) error {
	if obj == nil {
		return nil
	}
	port := getPort(obj)
	if port != 0 && (port < 1 || port > 65535) {
		return fmt.Errorf("%s %d is out of range (1-65535)", name, port)
	}
	return nil
}

// validateExtraArgs rejects extra args that start with any reserved flag prefix.
// BUG-QA-1 fix: prevents users from silently overriding operator-set flags
// like -port, -mdir, -block.listen etc., which would break probes/portals.
func validateExtraArgs(fieldName string, args []string, reserved []string) error {
	for _, arg := range args {
		for _, prefix := range reserved {
			if strings.HasPrefix(arg, prefix) {
				return fmt.Errorf("%s: flag %q conflicts with operator-managed flag %q; use the corresponding spec field instead",
					fieldName, arg, strings.TrimSuffix(prefix, "="))
			}
		}
	}
	return nil
}
