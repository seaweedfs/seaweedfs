package controller

// Adversarial tests for the SeaweedBlockCluster reconciler.
// Target: edge cases, race-like scenarios, input fuzzing, state transitions.

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/internal/resources"
)

// fullStackClusterWithVolume creates a full-stack cluster with both Master and Volume specs.
func fullStackClusterWithVolume() *blockv1alpha1.SeaweedBlockCluster {
	replicas := int32(1)
	return &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-full",
			Namespace: "default",
			UID:       "uid-fs",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Image:    "chrislusf/seaweedfs:latest",
			CSIImage: "sw-block-csi:local",
			Master: &blockv1alpha1.MasterSpec{
				Replicas: &replicas,
				Port:     9333,
				GRPCPort: 19333,
				Storage:  &blockv1alpha1.StorageSpec{Size: "5Gi"},
			},
			Volume: &blockv1alpha1.VolumeSpec{
				Replicas:        &replicas,
				Port:            8080,
				GRPCPort:        18080,
				BlockDir:        "/data1/block",
				BlockListenPort: 3260,
				Storage:         &blockv1alpha1.StorageSpec{Size: "50Gi"},
			},
			StorageClassName: "sw-block-full",
			CSINamespace:     "kube-system",
		},
	}
}

// =============================================================================
// QA-1: Cross-namespace resource conflict not detected.
//
// createOrUpdateCrossNamespace doesn't check ownership labels. If CR-A already
// created a CSI controller Deployment in kube-system, CR-B should not silently
// overwrite it.
// =============================================================================

func TestQA_CrossNamespace_ConflictNotDetected(t *testing.T) {
	scheme := testScheme()

	// CR-A creates resources first
	clusterA := csiOnlyCluster()
	clusterA.Name = "block-a"
	clusterA.UID = "uid-a"

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(clusterA).
		WithStatusSubresource(clusterA).
		Build()

	rA := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, rA, "block-a", "default") // finalizer
	reconcile(t, rA, "block-a", "default") // create resources

	ctx := context.Background()

	// Verify A owns the CSI controller
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "block-a-csi-controller", Namespace: "kube-system"}, &dep); err != nil {
		t.Fatalf("CR-A CSI controller should exist: %v", err)
	}
	if dep.Labels[blockv1alpha1.LabelOwnerName] != "block-a" {
		t.Fatalf("CSI controller should be owned by block-a, got %q", dep.Labels[blockv1alpha1.LabelOwnerName])
	}

	// CR-B tries to reconcile — its CSI controller has a DIFFERENT name (block-b-csi-controller)
	// so there's no direct conflict on CSI controller Deployment names. But the shared
	// cluster-scoped resources (CSIDriver, ClusterRole, CRB) SHOULD conflict.
	clusterB := csiOnlyCluster()
	clusterB.Name = "block-b"
	clusterB.UID = "uid-b"
	if err := c.Create(ctx, clusterB); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, clusterB); err != nil {
		// Status subresource may need explicit setup
	}

	rB := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, rB, "block-b", "default") // finalizer
	reconcile(t, rB, "block-b", "default") // attempt reconcile

	// CR-B should fail because cluster-scoped CSIDriver is owned by CR-A
	var updatedB blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "block-b", Namespace: "default"}, &updatedB); err != nil {
		t.Fatal(err)
	}
	if updatedB.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("BUG: CR-B phase = %q, want Failed (cluster-scoped conflict with CR-A)", updatedB.Status.Phase)
	}
}

// =============================================================================
// QA-2: ExtraArgs can override operator-managed flags.
//
// User sets ExtraArgs=["-port=1234"]. Since args are appended, the volume server
// sees both "-port=8080" and "-port=1234". SeaweedFS's flag parsing takes the
// LAST value, so user can silently override operator-managed ports. This could
// cause the readiness probe (targeting default port) to fail.
// =============================================================================

func TestQA_ExtraArgs_OverridesOperatorFlags(t *testing.T) {
	cluster := fullStackClusterWithVolume()
	cluster.Spec.Volume.ExtraArgs = []string{"-port=1234"}

	// BUG-QA-1 fix: validate() now rejects ExtraArgs that override operator flags
	err := validate(&cluster.Spec)
	if err == nil {
		t.Error("BUG: -port=1234 in volume ExtraArgs should be rejected by validation")
	} else if !strings.Contains(err.Error(), "conflicts with operator-managed flag") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestQA_ExtraArgs_OverridesOperatorFlags_Master(t *testing.T) {
	cluster := fullStackClusterWithVolume()
	cluster.Spec.Master.ExtraArgs = []string{"-port=5555", "-mdir=/evil"}

	// BUG-QA-1 fix: validate() now rejects ExtraArgs that override operator flags
	err := validate(&cluster.Spec)
	if err == nil {
		t.Error("BUG: -port=5555 in master ExtraArgs should be rejected by validation")
	} else if !strings.Contains(err.Error(), "conflicts with operator-managed flag") {
		t.Errorf("unexpected error: %v", err)
	}
}

// =============================================================================
// QA-3: Malformed rotation timestamp asymmetry.
//
// NeedsRotation: if rotateTS is unparseable → returns false (no rotation).
// If lastRotated is unparseable → returns true (forces rotation). This
// asymmetry means a malformed lastRotated annotation causes infinite rotation.
// =============================================================================

func TestQA_RotationTimestamp_MalformedLastRotated_ForcesInfiniteRotation(t *testing.T) {
	cluster := csiOnlyCluster()
	cluster.Annotations = map[string]string{
		blockv1alpha1.AnnotationRotateSecret: "2025-01-01T00:00:00Z",
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				blockv1alpha1.AnnotationLastRotated: "not-a-valid-timestamp",
			},
		},
	}

	if resources.NeedsRotation(cluster, secret) {
		t.Error("BUG: malformed lastRotated forces rotation. " +
			"If someone manually sets an invalid annotation, every reconcile will regenerate the CHAP password, " +
			"breaking all existing iSCSI sessions. Should return false for safety.")
	}
}

func TestQA_RotationTimestamp_MalformedRotateSecret_Skips(t *testing.T) {
	cluster := csiOnlyCluster()
	cluster.Annotations = map[string]string{
		blockv1alpha1.AnnotationRotateSecret: "garbage",
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				blockv1alpha1.AnnotationLastRotated: "2025-01-01T00:00:00Z",
			},
		},
	}

	// This is correct behavior: unparseable request → skip rotation
	if resources.NeedsRotation(cluster, secret) {
		t.Error("malformed rotateSecret should skip rotation")
	}
}

// =============================================================================
// QA-4: Long CR name exceeds K8s name limits.
//
// K8s resource names must be <= 253 chars. DNS labels (used by Services, Pods
// in StatefulSets) must be <= 63 chars. A long CR name could create invalid
// resource names.
// =============================================================================

func TestQA_LongCRName_ExceedsDNSLabelLimit(t *testing.T) {
	// DNS label limit is 63 chars. CR name + "-csi-controller" suffix = +16 chars
	// So a CR name of 48+ chars would exceed 63 when suffixed
	longName := strings.Repeat("a", 50)

	// BUG-QA-3 fix: validateName() now rejects names that would produce >63 char derived names
	err := validateName(longName)
	if err == nil {
		t.Error("BUG: 50-char CR name should be rejected (derived names exceed 63 chars)")
	} else if !strings.Contains(err.Error(), "too long") {
		t.Errorf("unexpected error: %v", err)
	}

	// 47 chars should be the max allowed (63 - 16 for "-csi-controller")
	okName := strings.Repeat("a", maxCRNameLength)
	if err := validateName(okName); err != nil {
		t.Errorf("name of %d chars should be valid: %v", maxCRNameLength, err)
	}
}

func TestQA_LongCRName_StatefulSetNames(t *testing.T) {
	longName := strings.Repeat("b", 50)
	replicas := int32(1)

	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      longName,
			Namespace: "default",
			UID:       "uid-long",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Master: &blockv1alpha1.MasterSpec{
				Replicas: &replicas,
				Storage:  &blockv1alpha1.StorageSpec{Size: "1Gi"},
			},
			Volume: &blockv1alpha1.VolumeSpec{
				Storage: &blockv1alpha1.StorageSpec{Size: "10Gi"},
			},
		},
	}
	applyDefaults(&cluster.Spec)

	masterSTS := resources.BuildMasterStatefulSet(cluster)
	// StatefulSet pod names are <sts-name>-<ordinal>, which must also fit in DNS labels
	podName := masterSTS.Name + "-0"
	if len(podName) > 63 {
		t.Errorf("BUG: master StatefulSet pod name %q is %d chars, exceeds DNS label limit",
			podName, len(podName))
	}

	volumeSTS := resources.BuildVolumeStatefulSet(cluster, "master:9333")
	podName = volumeSTS.Name + "-0"
	if len(podName) > 63 {
		t.Errorf("BUG: volume StatefulSet pod name %q is %d chars, exceeds DNS label limit",
			podName, len(podName))
	}
}

// =============================================================================
// QA-5: Replicas=0 gets misleading error message.
//
// validate() rejects replicas != 1 with "HA deferred to Phase 9C". But
// replicas=0 is not an HA request — it's either a mistake or a scale-to-zero.
// The error message is misleading.
// =============================================================================

func TestQA_MasterReplicas_Zero_MisleadingError(t *testing.T) {
	replicas := int32(0)
	spec := &blockv1alpha1.SeaweedBlockClusterSpec{
		Master: &blockv1alpha1.MasterSpec{Replicas: &replicas},
	}

	err := validate(spec)
	if err == nil {
		t.Fatal("replicas=0 should be rejected")
	}

	// The error should NOT mention "HA" or "Phase 9C" for replicas=0
	if strings.Contains(err.Error(), "HA") || strings.Contains(err.Error(), "9C") {
		t.Errorf("BUG: replicas=0 error message %q mentions HA/Phase 9C, "+
			"but zero replicas is not an HA request — it's invalid input. "+
			"Error should say 'replicas must be 1' or 'replicas must be >= 1'.",
			err.Error())
	}
}

// =============================================================================
// QA-6: Condition cleanup after Failed→Running transition.
//
// After a spec goes from invalid → valid, old failure conditions (ResourceConflict,
// ValidationFailed) should be removed. Test by first failing, then fixing.
// =============================================================================

func TestQA_ConditionCleanup_FailedToRunning(t *testing.T) {
	// Start with invalid spec (neither master nor masterRef)
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flip-flop", Namespace: "default", UID: "uid-flip",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Reconcile with invalid spec → should fail
	reconcile(t, r, "flip-flop", "default") // finalizer
	reconcile(t, r, "flip-flop", "default") // validate fails

	ctx := context.Background()
	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "flip-flop", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Fatalf("initial phase = %q, want Failed", updated.Status.Phase)
	}

	// Count failure conditions
	failCondCount := 0
	for _, cond := range updated.Status.Conditions {
		if cond.Type == blockv1alpha1.ConditionValidationFailed || cond.Type == blockv1alpha1.ConditionResourceConflict {
			failCondCount++
		}
	}
	if failCondCount == 0 {
		t.Fatal("should have at least one failure condition")
	}

	// Fix the spec
	updated.Spec.MasterRef = &blockv1alpha1.MasterRef{Address: "master:9333"}
	if err := c.Update(ctx, &updated); err != nil {
		t.Fatal(err)
	}

	// Reconcile again with valid spec
	reconcile(t, r, "flip-flop", "default")

	// Check that failure conditions are cleared
	if err := c.Get(ctx, types.NamespacedName{Name: "flip-flop", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}

	for _, cond := range updated.Status.Conditions {
		if cond.Type == blockv1alpha1.ConditionValidationFailed {
			t.Error("BUG: ValidationFailed condition not cleaned up after spec became valid")
		}
		if cond.Type == blockv1alpha1.ConditionResourceConflict {
			t.Error("BUG: ResourceConflict condition not cleaned up after spec became valid")
		}
	}
}

// =============================================================================
// QA-7: Condition duplication under repeated reconciles.
//
// Multiple reconciles should never produce duplicate conditions.
// =============================================================================

func TestQA_ConditionDeduplication(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Run many reconciles
	for i := 0; i < 10; i++ {
		reconcile(t, r, "test-block", "default")
	}

	ctx := context.Background()
	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}

	// Check for duplicates
	seen := make(map[string]int)
	for _, cond := range updated.Status.Conditions {
		seen[cond.Type]++
		if seen[cond.Type] > 1 {
			t.Errorf("BUG: duplicate condition type %q (%d occurrences)",
				cond.Type, seen[cond.Type])
		}
	}
}

// =============================================================================
// QA-8: CR name with dots/underscores could create invalid resource names.
//
// K8s names must match [a-z0-9]([a-z0-9.-]*[a-z0-9])? but some contexts
// are stricter. The operator blindly appends suffixes without sanitizing.
// =============================================================================

func TestQA_CRName_SpecialCharacters(t *testing.T) {
	// BUG-QA-5 fix: validateName() now checks RFC 1123 DNS label format.
	// Only lowercase alphanumeric + hyphens, must start/end with alphanumeric.
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"my-block", false},         // valid
		{"my--block", false},        // consecutive hyphens are valid in DNS labels
		{"-leading-hyphen", true},   // invalid: leading hyphen
		{"trailing-hyphen-", true},  // invalid: trailing hyphen
		{"my.block", true},          // dots are not valid in DNS labels (only in DNS names)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateName(tc.name)
			if tc.wantErr && err == nil {
				t.Errorf("name %q should be rejected by validateName()", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("name %q should be valid, got error: %v", tc.name, err)
			}
		})
	}
}

// =============================================================================
// QA-9: StorageClass adoption then deletion doesn't clean up adopted SC.
//
// If we adopt an existing StorageClass, then delete the CR, the finalizer
// should also delete the adopted StorageClass (it now has our labels).
// =============================================================================

func TestQA_AdoptedStorageClass_CleanedUpOnDeletion(t *testing.T) {
	// Pre-existing StorageClass without owner labels
	existingSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sw-block",
		},
		Provisioner: blockv1alpha1.CSIDriverName,
	}

	cluster := csiOnlyCluster()
	cluster.Spec.AdoptExistingStorageClass = true

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingSC).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-block", "default") // finalizer
	reconcile(t, r, "test-block", "default") // adopt SC

	ctx := context.Background()

	// Verify SC is now owned
	var sc storagev1.StorageClass
	if err := c.Get(ctx, types.NamespacedName{Name: "sw-block"}, &sc); err != nil {
		t.Fatal(err)
	}
	if sc.Labels[blockv1alpha1.LabelOwnerName] != "test-block" {
		t.Fatal("SC should be adopted")
	}

	// Now delete the CR — call cleanup directly
	var latest blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &latest); err != nil {
		t.Fatal(err)
	}

	if err := r.cleanupOwnedResources(ctx, &latest); err != nil {
		t.Fatal(err)
	}

	// The adopted StorageClass should be cleaned up
	err := c.Get(ctx, types.NamespacedName{Name: "sw-block"}, &sc)
	if !apierrors.IsNotFound(err) {
		t.Error("BUG: adopted StorageClass should be deleted during cleanup, but it still exists")
	}
}

// =============================================================================
// QA-10: Reconcile with empty string fields that bypass defaults.
//
// Spec fields like Image, CSIImage are checked for "" in applyDefaults.
// But what about whitespace-only values like " "? They wouldn't match ""
// and would create invalid K8s resources.
// =============================================================================

func TestQA_WhitespaceFields_BypassDefaults(t *testing.T) {
	cluster := csiOnlyCluster()
	cluster.Spec.Image = "  "      // whitespace only
	cluster.Spec.CSIImage = "\t"   // tab only

	applyDefaults(&cluster.Spec)

	// After defaults, whitespace strings should be replaced with defaults
	if strings.TrimSpace(cluster.Spec.Image) == "" && cluster.Spec.Image != blockv1alpha1.DefaultImage {
		t.Errorf("BUG: whitespace-only Image %q bypasses defaults. "+
			"applyDefaults should trim or check for whitespace.", cluster.Spec.Image)
	}
	if strings.TrimSpace(cluster.Spec.CSIImage) == "" && cluster.Spec.CSIImage != blockv1alpha1.DefaultCSIImage {
		t.Errorf("BUG: whitespace-only CSIImage %q bypasses defaults", cluster.Spec.CSIImage)
	}
}

// =============================================================================
// QA-11: Full-stack without Volume spec auto-creates Volume, but validate()
// has Volume port validation that could trigger on the auto-created Volume.
// =============================================================================

func TestQA_FullStackAutoVolume_DefaultsAppliedBeforeValidation(t *testing.T) {
	replicas := int32(1)
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "auto-vol", Namespace: "default", UID: "uid-av",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Master: &blockv1alpha1.MasterSpec{
				Replicas: &replicas,
			},
			// No Volume spec — reconciler auto-creates it
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// This should not panic or fail — auto-created Volume should get defaults
	reconcile(t, r, "auto-vol", "default") // finalizer
	reconcile(t, r, "auto-vol", "default") // create resources

	ctx := context.Background()
	var volSts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "auto-vol-volume", Namespace: "default"}, &volSts); err != nil {
		t.Errorf("auto-created Volume StatefulSet should exist: %v", err)
	}
}

// =============================================================================
// QA-12: Multiple rapid reconciles of the same CR — no resource version conflicts.
//
// Simulate rapid reconciles to check for ResourceVersion-related issues.
// =============================================================================

func TestQA_RapidReconcile_NoResourceVersionConflict(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Run 20 rapid reconciles — should all succeed without error
	for i := 0; i < 20; i++ {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "test-block", Namespace: "default"},
		})
		if err != nil {
			t.Fatalf("reconcile #%d failed: %v", i, err)
		}
	}
}

// =============================================================================
// QA-13: Cleanup with missing resources doesn't error.
//
// If resources were already manually deleted before the finalizer runs,
// cleanup should succeed (not fail on NotFound).
// =============================================================================

func TestQA_Cleanup_MissingResources_NoError(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()

	// Don't create any resources — simulate pre-deletion of everything
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Cleanup should not error even though no resources exist
	if err := r.cleanupOwnedResources(context.Background(), cluster); err != nil {
		t.Errorf("BUG: cleanup with missing resources should succeed, got: %v", err)
	}
}

// =============================================================================
// QA-14: Volume storage size "0" — valid Quantity but nonsensical.
// =============================================================================

func TestQA_StorageSize_Zero(t *testing.T) {
	replicas := int32(1)
	spec := &blockv1alpha1.SeaweedBlockClusterSpec{
		Master: &blockv1alpha1.MasterSpec{
			Replicas: &replicas,
			Storage:  &blockv1alpha1.StorageSpec{Size: "0"},
		},
	}

	// "0" is a valid resource.Quantity but creates a 0-byte PVC.
	// validate() should reject it or the reconciler should handle it.
	err := validate(spec)
	if err == nil {
		// If validation passes, verify the builder handles it
		applyDefaults(spec)
		sts := resources.BuildMasterStatefulSet(&blockv1alpha1.SeaweedBlockCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "zero-pvc", Namespace: "default"},
			Spec:       *spec,
		})
		if len(sts.Spec.VolumeClaimTemplates) > 0 {
			size := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
			if size.IsZero() {
				t.Error("BUG: 0-byte PVC request will be rejected by most storage provisioners. " +
					"Consider validating storage size > 0.")
			}
		}
	}
}

// =============================================================================
// QA-15: CSI-only mode with csiNamespace same as CR namespace.
//
// When csiNamespace == CR namespace, cross-namespace logic is used but the
// resources are actually same-namespace. Verify this works correctly.
// =============================================================================

func TestQA_CSINamespace_SameAsCRNamespace(t *testing.T) {
	cluster := csiOnlyCluster()
	cluster.Spec.CSINamespace = "default" // same as CR namespace

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-block", "default") // finalizer
	reconcile(t, r, "test-block", "default") // resources

	ctx := context.Background()

	// CSI controller should be in "default" namespace
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-csi-controller", Namespace: "default"}, &dep); err != nil {
		t.Fatalf("CSI controller should be in 'default' namespace: %v", err)
	}

	// Cleanup should still work (even though it's same-namespace)
	var latest blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &latest); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupOwnedResources(ctx, &latest); err != nil {
		t.Errorf("cleanup with same-namespace CSI should work: %v", err)
	}

	// CSI controller should be deleted
	err := c.Get(ctx, types.NamespacedName{Name: "test-block-csi-controller", Namespace: "default"}, &dep)
	if !apierrors.IsNotFound(err) {
		t.Error("CSI controller should be deleted during cleanup")
	}
}

// =============================================================================
// QA-16: Ownership label tampering on cluster-scoped resources.
//
// If someone manually changes owner labels on a cluster-scoped resource to
// point to a different CR, the real owner should detect this as a conflict.
// =============================================================================

func TestQA_OwnershipLabel_Tampering(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-block", "default") // finalizer
	reconcile(t, r, "test-block", "default") // create

	ctx := context.Background()

	// Tamper with CSIDriver labels — change owner to fake CR
	var csiDriver storagev1.CSIDriver
	if err := c.Get(ctx, types.NamespacedName{Name: blockv1alpha1.CSIDriverName}, &csiDriver); err != nil {
		t.Fatal(err)
	}
	csiDriver.Labels[blockv1alpha1.LabelOwnerName] = "evil-block"
	csiDriver.Labels[blockv1alpha1.LabelOwnerNamespace] = "evil-ns"
	if err := c.Update(ctx, &csiDriver); err != nil {
		t.Fatal(err)
	}

	// Next reconcile should detect conflict
	reconcile(t, r, "test-block", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}

	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("BUG: label tampering should cause conflict detection, phase = %q, want Failed",
			updated.Status.Phase)
	}
}

// =============================================================================
// QA-17: Cleanup doesn't delete resources owned by a different CR.
//
// If two CRs exist and CR-A is deleted, cleanup should NOT delete resources
// owned by CR-B.
// =============================================================================

func TestQA_Cleanup_DoesNotDeleteOtherCRResources(t *testing.T) {
	scheme := testScheme()

	// Create a CSI controller deployment owned by CR-B
	clusterB := csiOnlyCluster()
	clusterB.Name = "block-b"
	clusterB.UID = "uid-b"
	depB := resources.BuildCSIControllerDeployment(clusterB, "master:9333", "kube-system")

	// Create CR-A
	clusterA := csiOnlyCluster()
	clusterA.Name = "block-a"
	clusterA.UID = "uid-a"

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(clusterA, depB).
		WithStatusSubresource(clusterA).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Cleanup for CR-A should not touch CR-B's deployment
	if err := r.cleanupOwnedResources(context.Background(), clusterA); err != nil {
		t.Fatal(err)
	}

	// CR-B's deployment should still exist
	ctx := context.Background()
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "block-b-csi-controller", Namespace: "kube-system"}, &dep); err != nil {
		t.Error("BUG: cleanup for CR-A should not delete CR-B's resources")
	}
}

// =============================================================================
// QA-18: Secret rotation timestamp at exact same second — no rotation.
//
// If rotateSecret and lastRotated are the same timestamp, NeedsRotation
// should return false (rotation already happened).
// =============================================================================

func TestQA_RotationTimestamp_ExactSame_NoRotation(t *testing.T) {
	ts := "2025-06-15T12:00:00Z"

	cluster := csiOnlyCluster()
	cluster.Annotations = map[string]string{
		blockv1alpha1.AnnotationRotateSecret: ts,
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				blockv1alpha1.AnnotationLastRotated: ts,
			},
		},
	}

	if resources.NeedsRotation(cluster, secret) {
		t.Error("BUG: same timestamp should not trigger rotation (already done)")
	}
}

