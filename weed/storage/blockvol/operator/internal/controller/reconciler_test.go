package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/internal/resources"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = blockv1alpha1.AddToScheme(s)
	return s
}

func csiOnlyCluster() *blockv1alpha1.SeaweedBlockCluster {
	return &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-block",
			Namespace: "default",
			UID:       "uid-123",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			CSIImage: "sw-block-csi:local",
			MasterRef: &blockv1alpha1.MasterRef{
				Address: "master.default:9333",
			},
			StorageClassName: "sw-block",
			CSINamespace:     "kube-system",
		},
	}
}

func fullStackCluster() *blockv1alpha1.SeaweedBlockCluster {
	replicas := int32(1)
	return &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-full",
			Namespace: "default",
			UID:       "uid-456",
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
			StorageClassName: "sw-block-full",
			CSINamespace:     "kube-system",
		},
	}
}

func reconcile(t *testing.T, r *Reconciler, name, ns string) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
	})
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	return result
}

func ensureNamespace(objects []runtime.Object) []runtime.Object {
	for _, o := range objects {
		if ns, ok := o.(*corev1.Namespace); ok {
			if ns.Name == "kube-system" {
				return objects
			}
		}
	}
	return append(objects, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-system"},
	})
}

// --- Test: CSI-only mode creates all CSI sub-resources ---

func TestReconcile_CSIOnly_CreatesResources(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// First reconcile: adds finalizer
	reconcile(t, r, "test-block", "default")

	// Second reconcile: creates resources (after finalizer is persisted)
	reconcile(t, r, "test-block", "default")

	ctx := context.Background()

	// Verify finalizer was added
	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	hasFinalizer := false
	for _, f := range updated.Finalizers {
		if f == blockv1alpha1.FinalizerName {
			hasFinalizer = true
		}
	}
	if !hasFinalizer {
		t.Error("missing finalizer")
	}

	// CSI Controller Deployment
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-csi-controller", Namespace: "kube-system"}, &dep); err != nil {
		t.Errorf("CSI controller not created: %v", err)
	}

	// CSI Node DaemonSet
	var ds appsv1.DaemonSet
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-csi-node", Namespace: "kube-system"}, &ds); err != nil {
		t.Errorf("CSI node not created: %v", err)
	}

	// ServiceAccount
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Name: "sw-block-csi", Namespace: "kube-system"}, &sa); err != nil {
		t.Errorf("ServiceAccount not created: %v", err)
	}

	// ClusterRole
	var cr rbacv1.ClusterRole
	if err := c.Get(ctx, types.NamespacedName{Name: "sw-block-csi"}, &cr); err != nil {
		t.Errorf("ClusterRole not created: %v", err)
	}

	// ClusterRoleBinding
	var crb rbacv1.ClusterRoleBinding
	if err := c.Get(ctx, types.NamespacedName{Name: "sw-block-csi"}, &crb); err != nil {
		t.Errorf("ClusterRoleBinding not created: %v", err)
	}

	// CSIDriver
	var csiDriver storagev1.CSIDriver
	if err := c.Get(ctx, types.NamespacedName{Name: blockv1alpha1.CSIDriverName}, &csiDriver); err != nil {
		t.Errorf("CSIDriver not created: %v", err)
	}

	// StorageClass
	var sc storagev1.StorageClass
	if err := c.Get(ctx, types.NamespacedName{Name: "sw-block"}, &sc); err != nil {
		t.Errorf("StorageClass not created: %v", err)
	}

	// CHAP Secret
	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-chap", Namespace: "kube-system"}, &secret); err != nil {
		t.Errorf("CHAP secret not created: %v", err)
	}

	// No master or volume StatefulSets in CSI-only mode
	var masterSts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-master", Namespace: "default"}, &masterSts); !apierrors.IsNotFound(err) {
		t.Error("master StatefulSet should not exist in CSI-only mode")
	}
}

// --- Test: Invalid spec (both master+masterRef) → Failed ---

func TestReconcile_BothMasterAndRef_Failed(t *testing.T) {
	replicas := int32(1)
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bad-block",
			Namespace: "default",
			UID:       "uid-bad",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Master:    &blockv1alpha1.MasterSpec{Replicas: &replicas},
			MasterRef: &blockv1alpha1.MasterRef{Address: "x:9333"},
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// First reconcile: add finalizer
	reconcile(t, r, "bad-block", "default")
	// Second: validate
	reconcile(t, r, "bad-block", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(context.Background(), types.NamespacedName{Name: "bad-block", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", updated.Status.Phase)
	}
}

// --- Test: Neither master nor masterRef → Failed ---

func TestReconcile_NeitherMasterNorRef_Failed(t *testing.T) {
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-block",
			Namespace: "default",
			UID:       "uid-empty",
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
	reconcile(t, r, "empty-block", "default")
	reconcile(t, r, "empty-block", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "empty-block", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", updated.Status.Phase)
	}
}

// --- Test: Master replicas > 1 → Failed ---

func TestReconcile_MasterReplicasGT1_Failed(t *testing.T) {
	replicas := int32(3)
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ha-block", Namespace: "default", UID: "uid-ha",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Master: &blockv1alpha1.MasterSpec{Replicas: &replicas},
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "ha-block", "default")
	reconcile(t, r, "ha-block", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "ha-block", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", updated.Status.Phase)
	}
}

// --- Test: Multi-CR safety — second CR with conflicting cluster-scoped resources ---

func TestReconcile_MultiCR_ConflictDetected(t *testing.T) {
	// Pre-create cluster-scoped resources owned by another CR
	clusterA := csiOnlyCluster()
	clusterA.Name = "block-a"
	clusterA.UID = "uid-a"

	// Create CSIDriver owned by block-a
	csiDriver := resources.BuildCSIDriverResource(clusterA)

	clusterB := csiOnlyCluster()
	clusterB.Name = "block-b"
	clusterB.UID = "uid-b"

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(clusterB, csiDriver).
		WithStatusSubresource(clusterB).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "block-b", "default") // finalizer
	reconcile(t, r, "block-b", "default") // reconcile

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "block-b", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed (conflict)", updated.Status.Phase)
	}

	// Should have ResourceConflict condition
	found := false
	for _, c := range updated.Status.Conditions {
		if c.Type == blockv1alpha1.ConditionResourceConflict && c.Status == metav1.ConditionTrue {
			found = true
		}
	}
	if !found {
		t.Error("missing ResourceConflict condition")
	}
}

// --- Test: Reconcile idempotency ---

func TestReconcile_Idempotent(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Run 3 reconciles
	for i := 0; i < 3; i++ {
		reconcile(t, r, "test-block", "default")
	}

	// Should have exactly 1 CSI controller deployment
	ctx := context.Background()
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-csi-controller", Namespace: "kube-system"}, &dep); err != nil {
		t.Errorf("CSI controller missing after idempotent reconciles: %v", err)
	}
}

// --- Test: Secret rotation via annotation ---

func TestReconcile_SecretRotation(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Create resources
	reconcile(t, r, "test-block", "default")
	reconcile(t, r, "test-block", "default")

	// Get original password
	ctx := context.Background()
	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-chap", Namespace: "kube-system"}, &secret); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	originalPassword := string(secret.Data["password"])

	// Add rotation annotation
	var updated blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[blockv1alpha1.AnnotationRotateSecret] = "2099-01-01T00:00:00Z"
	if err := c.Update(ctx, &updated); err != nil {
		t.Fatal(err)
	}

	// Reconcile again
	reconcile(t, r, "test-block", "default")

	// Check password changed
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block-chap", Namespace: "kube-system"}, &secret); err != nil {
		t.Fatal(err)
	}
	newPassword := string(secret.Data["password"])
	if newPassword == originalPassword {
		t.Error("password should have changed after rotation")
	}
}

// --- Test: User-provided secret ref skips auto-generation ---

func TestReconcile_UserProvidedSecret_SkipsAutoGen(t *testing.T) {
	cluster := csiOnlyCluster()
	cluster.Spec.Auth = &blockv1alpha1.AuthSpec{
		SecretRef: &corev1.LocalObjectReference{Name: "my-custom-secret"},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-block", "default")
	reconcile(t, r, "test-block", "default")

	// Auto-generated secret should NOT exist
	var secret corev1.Secret
	err := c.Get(context.Background(), types.NamespacedName{Name: "test-block-chap", Namespace: "kube-system"}, &secret)
	if !apierrors.IsNotFound(err) {
		t.Error("auto-generated secret should not exist when secretRef is provided")
	}
}

// --- Test: Full-stack mode creates master + volume StatefulSets ---

func TestReconcile_FullStack_CreatesMasterAndVolume(t *testing.T) {
	cluster := fullStackCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-full", "default")
	reconcile(t, r, "test-full", "default")

	ctx := context.Background()

	// Master Service
	var masterSvc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Name: "test-full-master", Namespace: "default"}, &masterSvc); err != nil {
		t.Errorf("master service not created: %v", err)
	}

	// Master StatefulSet
	var masterSts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "test-full-master", Namespace: "default"}, &masterSts); err != nil {
		t.Errorf("master StatefulSet not created: %v", err)
	}

	// Volume Service
	var volSvc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Name: "test-full-volume", Namespace: "default"}, &volSvc); err != nil {
		t.Errorf("volume service not created: %v", err)
	}

	// Volume StatefulSet
	var volSts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "test-full-volume", Namespace: "default"}, &volSts); err != nil {
		t.Errorf("volume StatefulSet not created: %v", err)
	}

	// CSI resources also created
	var dep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-full-csi-controller", Namespace: "kube-system"}, &dep); err != nil {
		t.Errorf("CSI controller not created in full-stack mode: %v", err)
	}
}

// --- Test: Volume without master → Failed ---

func TestReconcile_VolumeWithoutMaster_Failed(t *testing.T) {
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol-only", Namespace: "default", UID: "uid-vol",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			MasterRef: &blockv1alpha1.MasterRef{Address: "m:9333"},
			Volume:    &blockv1alpha1.VolumeSpec{},
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "vol-only", "default")
	reconcile(t, r, "vol-only", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "vol-only", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", updated.Status.Phase)
	}
}

// --- Test: Defaults are applied ---

func TestApplyDefaults(t *testing.T) {
	spec := &blockv1alpha1.SeaweedBlockClusterSpec{}
	applyDefaults(spec)

	if spec.Image != blockv1alpha1.DefaultImage {
		t.Errorf("image = %q", spec.Image)
	}
	if spec.CSIImage != blockv1alpha1.DefaultCSIImage {
		t.Errorf("csiImage = %q", spec.CSIImage)
	}
	if spec.StorageClassName != blockv1alpha1.DefaultStorageClassName {
		t.Errorf("storageClassName = %q", spec.StorageClassName)
	}
	if spec.CSINamespace != blockv1alpha1.DefaultCSINamespace {
		t.Errorf("csiNamespace = %q", spec.CSINamespace)
	}
	if spec.CSI == nil {
		t.Fatal("CSI should be initialized")
	}
	if spec.CSI.ControllerReplicas == nil || *spec.CSI.ControllerReplicas != 1 {
		t.Error("CSI controller replicas should default to 1")
	}
}

func TestApplyDefaults_MasterAndVolume(t *testing.T) {
	spec := &blockv1alpha1.SeaweedBlockClusterSpec{
		Master: &blockv1alpha1.MasterSpec{},
		Volume: &blockv1alpha1.VolumeSpec{},
	}
	applyDefaults(spec)

	if spec.Master.Replicas == nil || *spec.Master.Replicas != 1 {
		t.Error("master replicas should default to 1")
	}
	if spec.Master.Port != blockv1alpha1.DefaultMasterPort {
		t.Errorf("master port = %d", spec.Master.Port)
	}
	if spec.Volume.Port != blockv1alpha1.DefaultVolumePort {
		t.Errorf("volume port = %d", spec.Volume.Port)
	}
	if spec.Volume.BlockDir != blockv1alpha1.DefaultBlockDir {
		t.Errorf("blockDir = %q", spec.Volume.BlockDir)
	}
	if spec.Volume.BlockListenPort != blockv1alpha1.DefaultBlockListenPort {
		t.Errorf("blockListenPort = %d", spec.Volume.BlockListenPort)
	}
}

// --- Test: Validation rules ---

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		spec    blockv1alpha1.SeaweedBlockClusterSpec
		wantErr string
	}{
		{
			name: "both master and masterRef",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				Master:    &blockv1alpha1.MasterSpec{},
				MasterRef: &blockv1alpha1.MasterRef{Address: "x:9333"},
			},
			wantErr: "mutually exclusive",
		},
		{
			name:    "neither master nor masterRef",
			spec:    blockv1alpha1.SeaweedBlockClusterSpec{},
			wantErr: "one of spec.master or spec.masterRef is required",
		},
		{
			name: "volume without master",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				MasterRef: &blockv1alpha1.MasterRef{Address: "x:9333"},
				Volume:    &blockv1alpha1.VolumeSpec{},
			},
			wantErr: "spec.volume requires spec.master",
		},
		{
			name: "master replicas > 1",
			spec: func() blockv1alpha1.SeaweedBlockClusterSpec {
				r := int32(3)
				return blockv1alpha1.SeaweedBlockClusterSpec{
					Master: &blockv1alpha1.MasterSpec{Replicas: &r},
				}
			}(),
			wantErr: "deferred to Phase 9C",
		},
		{
			name: "valid CSI-only",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				MasterRef: &blockv1alpha1.MasterRef{Address: "m:9333"},
			},
		},
		{
			name: "valid full-stack",
			spec: func() blockv1alpha1.SeaweedBlockClusterSpec {
				r := int32(1)
				return blockv1alpha1.SeaweedBlockClusterSpec{
					Master: &blockv1alpha1.MasterSpec{Replicas: &r},
				}
			}(),
		},
		{
			name: "empty masterRef address",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				MasterRef: &blockv1alpha1.MasterRef{Address: ""},
			},
			wantErr: "must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(&tt.spec)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tt.wantErr)
				return
			}
			if !containsString(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// --- Test: Finalizer cleanup (deletion) ---

func TestReconcile_Deletion_CleansUpClusterScoped(t *testing.T) {
	cluster := csiOnlyCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// Create resources
	reconcile(t, r, "test-block", "default")
	reconcile(t, r, "test-block", "default")

	ctx := context.Background()

	// Verify CSIDriver exists
	var csiDriver storagev1.CSIDriver
	if err := c.Get(ctx, types.NamespacedName{Name: blockv1alpha1.CSIDriverName}, &csiDriver); err != nil {
		t.Fatalf("CSIDriver should exist: %v", err)
	}

	// Mark for deletion (simulate kubectl delete)
	var latest blockv1alpha1.SeaweedBlockCluster
	if err := c.Get(ctx, types.NamespacedName{Name: "test-block", Namespace: "default"}, &latest); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	latest.DeletionTimestamp = &now

	// We can't directly set DeletionTimestamp on fake client via Update,
	// so we'll test the cleanup function directly
	if err := r.cleanupOwnedResources(ctx, &latest); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// CSIDriver should be deleted
	err := c.Get(ctx, types.NamespacedName{Name: blockv1alpha1.CSIDriverName}, &csiDriver)
	if !apierrors.IsNotFound(err) {
		t.Error("CSIDriver should be deleted after cleanup")
	}

	// ClusterRole should be deleted
	var cr rbacv1.ClusterRole
	err = c.Get(ctx, types.NamespacedName{Name: resources.ClusterRoleName()}, &cr)
	if !apierrors.IsNotFound(err) {
		t.Error("ClusterRole should be deleted after cleanup")
	}

	// StorageClass should be deleted
	var sc storagev1.StorageClass
	err = c.Get(ctx, types.NamespacedName{Name: "sw-block"}, &sc)
	if !apierrors.IsNotFound(err) {
		t.Error("StorageClass should be deleted after cleanup")
	}

	// H1 fix: cross-namespace CSI resources should also be deleted
	var dep appsv1.Deployment
	err = c.Get(ctx, types.NamespacedName{Name: "test-block-csi-controller", Namespace: "kube-system"}, &dep)
	if !apierrors.IsNotFound(err) {
		t.Error("CSI controller Deployment should be deleted after cleanup")
	}

	var ds appsv1.DaemonSet
	err = c.Get(ctx, types.NamespacedName{Name: "test-block-csi-node", Namespace: "kube-system"}, &ds)
	if !apierrors.IsNotFound(err) {
		t.Error("CSI node DaemonSet should be deleted after cleanup")
	}

	var sa corev1.ServiceAccount
	err = c.Get(ctx, types.NamespacedName{Name: "sw-block-csi", Namespace: "kube-system"}, &sa)
	if !apierrors.IsNotFound(err) {
		t.Error("ServiceAccount should be deleted after cleanup")
	}

	var secret corev1.Secret
	err = c.Get(ctx, types.NamespacedName{Name: "test-block-chap", Namespace: "kube-system"}, &secret)
	if !apierrors.IsNotFound(err) {
		t.Error("CHAP Secret should be deleted after cleanup")
	}
}

// --- Test: StorageClass adoption ---

func TestReconcile_StorageClass_AdoptExisting(t *testing.T) {
	// Pre-existing SC without owner labels
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
	reconcile(t, r, "test-block", "default")
	reconcile(t, r, "test-block", "default")

	// SC should now have owner labels
	var sc storagev1.StorageClass
	if err := c.Get(context.Background(), types.NamespacedName{Name: "sw-block"}, &sc); err != nil {
		t.Fatal(err)
	}
	if sc.Labels[blockv1alpha1.LabelOwnerName] != "test-block" {
		t.Error("StorageClass should be adopted with owner labels")
	}
}

func TestReconcile_StorageClass_OrphanWithoutAdopt_Failed(t *testing.T) {
	existingSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sw-block",
		},
		Provisioner: blockv1alpha1.CSIDriverName,
	}

	cluster := csiOnlyCluster()
	// AdoptExistingStorageClass is false by default

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingSC).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-block", "default")
	reconcile(t, r, "test-block", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-block", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed for orphan SC without adopt", updated.Status.Phase)
	}
}

// --- Test: CR not found returns nil ---

func TestReconcile_NotFound_NoError(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.Requeue {
		t.Error("should not requeue for missing CR")
	}
}

// --- Test: H2 — invalid storage size is caught by validation ---

func TestReconcile_InvalidStorageSize_Failed(t *testing.T) {
	replicas := int32(1)
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bad-size", Namespace: "default", UID: "uid-bs",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			Master: &blockv1alpha1.MasterSpec{
				Replicas: &replicas,
				Storage:  &blockv1alpha1.StorageSpec{Size: "not-a-size"},
			},
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "bad-size", "default")
	reconcile(t, r, "bad-size", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "bad-size", Namespace: "default"}, &updated)
	if updated.Status.Phase != blockv1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed for invalid storage size", updated.Status.Phase)
	}
}

// --- Test: M4 — validation uses ConditionValidationFailed, not ResourceConflict ---

func TestReconcile_ValidationUsesCorrectCondition(t *testing.T) {
	cluster := &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "val-cond", Namespace: "default", UID: "uid-vc",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{}, // neither master nor masterRef
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "val-cond", "default")
	reconcile(t, r, "val-cond", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "val-cond", Namespace: "default"}, &updated)

	// Should use ValidationFailed, NOT ResourceConflict
	hasValidation := false
	hasConflict := false
	for _, cond := range updated.Status.Conditions {
		if cond.Type == blockv1alpha1.ConditionValidationFailed {
			hasValidation = true
		}
		if cond.Type == blockv1alpha1.ConditionResourceConflict {
			hasConflict = true
		}
	}
	if !hasValidation {
		t.Error("validation errors should use ConditionValidationFailed")
	}
	if hasConflict {
		t.Error("validation errors should NOT use ConditionResourceConflict")
	}
}

// --- Test: L3 — port validation ---

func TestValidate_PortRange(t *testing.T) {
	replicas := int32(1)
	tests := []struct {
		name    string
		spec    blockv1alpha1.SeaweedBlockClusterSpec
		wantErr bool
	}{
		{
			name: "valid port",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				Master: &blockv1alpha1.MasterSpec{Replicas: &replicas, Port: 9333},
			},
		},
		{
			name: "port 0 is valid (uses default)",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				Master: &blockv1alpha1.MasterSpec{Replicas: &replicas},
			},
		},
		{
			name: "negative port",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				Master: &blockv1alpha1.MasterSpec{Replicas: &replicas, Port: -1},
			},
			wantErr: true,
		},
		{
			name: "port too high",
			spec: blockv1alpha1.SeaweedBlockClusterSpec{
				Master: &blockv1alpha1.MasterSpec{Replicas: &replicas, Port: 70000},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(&tt.spec)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// --- Test: L1 — FQDN master address ---

func TestReconcile_FullStack_UsesFQDN(t *testing.T) {
	cluster := fullStackCluster()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	reconcile(t, r, "test-full", "default")
	reconcile(t, r, "test-full", "default")

	var updated blockv1alpha1.SeaweedBlockCluster
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-full", Namespace: "default"}, &updated)

	expected := "test-full-master.default.svc.cluster.local:9333"
	if updated.Status.MasterAddress != expected {
		t.Errorf("masterAddress = %q, want %q", updated.Status.MasterAddress, expected)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
