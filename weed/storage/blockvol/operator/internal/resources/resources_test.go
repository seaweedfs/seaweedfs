package resources

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// --- Helpers ---

func testCluster() *blockv1alpha1.SeaweedBlockCluster {
	return &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-block",
			Namespace: "default",
			UID:       "test-uid-123",
		},
		Spec: blockv1alpha1.SeaweedBlockClusterSpec{
			CSIImage: "sw-block-csi:local",
			MasterRef: &blockv1alpha1.MasterRef{
				Address: "seaweedfs-master.default:9333",
			},
			StorageClassName: "sw-block",
			CSINamespace:     "kube-system",
		},
	}
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func containsArg(args []string, target string) bool {
	for _, a := range args {
		if a == target {
			return true
		}
	}
	return false
}

// --- CSI Controller Deployment Tests ---

func TestCSIControllerDeployment_GoldenConformance(t *testing.T) {
	cluster := testCluster()
	dep := BuildCSIControllerDeployment(cluster, "seaweedfs-master.default:9333", "kube-system")

	// Name and namespace match reference
	if dep.Name != "my-block-csi-controller" {
		t.Errorf("name = %q, want my-block-csi-controller", dep.Name)
	}
	if dep.Namespace != "kube-system" {
		t.Errorf("namespace = %q, want kube-system", dep.Namespace)
	}

	// Replicas
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Error("replicas should be 1")
	}

	// Service account
	if dep.Spec.Template.Spec.ServiceAccountName != "sw-block-csi" {
		t.Errorf("serviceAccountName = %q, want sw-block-csi", dep.Spec.Template.Spec.ServiceAccountName)
	}

	// Container: block-csi
	csi := findContainer(dep.Spec.Template.Spec.Containers, "block-csi")
	if csi == nil {
		t.Fatal("missing block-csi container")
	}
	if csi.Image != "sw-block-csi:local" {
		t.Errorf("block-csi image = %q, want sw-block-csi:local", csi.Image)
	}
	if !containsArg(csi.Args, "-endpoint=unix:///csi/csi.sock") {
		t.Error("missing -endpoint arg")
	}
	if !containsArg(csi.Args, "-mode=controller") {
		t.Error("missing -mode=controller arg")
	}
	if !containsArg(csi.Args, "-master=seaweedfs-master.default:9333") {
		t.Error("missing -master arg")
	}
	if !containsArg(csi.Args, "-node-id=$(NODE_NAME)") {
		t.Error("missing -node-id arg")
	}

	// Container: csi-provisioner
	prov := findContainer(dep.Spec.Template.Spec.Containers, "csi-provisioner")
	if prov == nil {
		t.Fatal("missing csi-provisioner container")
	}
	if prov.Image != blockv1alpha1.DefaultProvisionerImage {
		t.Errorf("provisioner image = %q, want %s", prov.Image, blockv1alpha1.DefaultProvisionerImage)
	}
	if !containsArg(prov.Args, "--csi-address=/csi/csi.sock") {
		t.Error("missing --csi-address arg")
	}
	if !containsArg(prov.Args, "--feature-gates=Topology=true") {
		t.Error("missing --feature-gates arg")
	}

	// Volume: socket-dir (emptyDir)
	socketVol := findVolume(dep.Spec.Template.Spec.Volumes, "socket-dir")
	if socketVol == nil {
		t.Fatal("missing socket-dir volume")
	}
	if socketVol.EmptyDir == nil {
		t.Error("socket-dir should be emptyDir")
	}

	// Cross-namespace ownership: uses labels, not ownerRef
	if len(dep.OwnerReferences) != 0 {
		t.Errorf("CSI controller should NOT have ownerRef (cross-namespace); got %d", len(dep.OwnerReferences))
	}
	if dep.Labels[blockv1alpha1.LabelOwnerNamespace] != "default" {
		t.Error("missing owner-namespace label on CSI controller")
	}
	if dep.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label on CSI controller")
	}
}

func TestCSIControllerDeployment_CustomImages(t *testing.T) {
	cluster := testCluster()
	replicas := int32(2)
	cluster.Spec.CSI = &blockv1alpha1.CSISpec{
		ControllerReplicas: &replicas,
		ProvisionerImage:   "custom-provisioner:v1",
	}
	cluster.Spec.CSIImage = "custom-csi:v2"

	dep := BuildCSIControllerDeployment(cluster, "master:9333", "kube-system")

	if *dep.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want 2", *dep.Spec.Replicas)
	}
	csi := findContainer(dep.Spec.Template.Spec.Containers, "block-csi")
	if csi.Image != "custom-csi:v2" {
		t.Errorf("csi image = %q, want custom-csi:v2", csi.Image)
	}
	prov := findContainer(dep.Spec.Template.Spec.Containers, "csi-provisioner")
	if prov.Image != "custom-provisioner:v1" {
		t.Errorf("provisioner image = %q, want custom-provisioner:v1", prov.Image)
	}
}

// --- CSI Node DaemonSet Tests ---

func TestCSINodeDaemonSet_GoldenConformance(t *testing.T) {
	cluster := testCluster()
	ds := BuildCSINodeDaemonSet(cluster, "kube-system")

	// Name and namespace
	if ds.Name != "my-block-csi-node" {
		t.Errorf("name = %q, want my-block-csi-node", ds.Name)
	}
	if ds.Namespace != "kube-system" {
		t.Errorf("namespace = %q, want kube-system", ds.Namespace)
	}

	spec := ds.Spec.Template.Spec

	// hostNetwork and hostPID per reference
	if !spec.HostNetwork {
		t.Error("hostNetwork should be true")
	}
	if !spec.HostPID {
		t.Error("hostPID should be true")
	}

	// block-csi container: privileged
	csi := findContainer(spec.Containers, "block-csi")
	if csi == nil {
		t.Fatal("missing block-csi container")
	}
	if csi.SecurityContext == nil || csi.SecurityContext.Privileged == nil || !*csi.SecurityContext.Privileged {
		t.Error("block-csi must be privileged")
	}

	// Volume mounts match reference
	for _, expected := range []string{"socket-dir", "kubelet-dir", "dev", "iscsi-dir"} {
		if findVolumeMount(csi.VolumeMounts, expected) == nil {
			t.Errorf("missing volume mount %q on block-csi", expected)
		}
	}

	// kubelet-dir mount propagation
	kubeletMount := findVolumeMount(csi.VolumeMounts, "kubelet-dir")
	if kubeletMount.MountPropagation == nil || *kubeletMount.MountPropagation != corev1.MountPropagationBidirectional {
		t.Error("kubelet-dir mount propagation should be Bidirectional")
	}

	// Registrar container
	reg := findContainer(spec.Containers, "csi-node-driver-registrar")
	if reg == nil {
		t.Fatal("missing csi-node-driver-registrar container")
	}
	expectedRegPath := "--kubelet-registration-path=/var/lib/kubelet/plugins/block.csi.seaweedfs.com/csi.sock"
	if !containsArg(reg.Args, expectedRegPath) {
		t.Errorf("missing registrar arg %q", expectedRegPath)
	}

	// Volumes: socket-dir should be hostPath DirectoryOrCreate
	socketVol := findVolume(spec.Volumes, "socket-dir")
	if socketVol == nil || socketVol.HostPath == nil {
		t.Fatal("socket-dir should be hostPath")
	}
	expectedPath := "/var/lib/kubelet/plugins/block.csi.seaweedfs.com"
	if socketVol.HostPath.Path != expectedPath {
		t.Errorf("socket-dir path = %q, want %q", socketVol.HostPath.Path, expectedPath)
	}

	// registration-dir volume
	regVol := findVolume(spec.Volumes, "registration-dir")
	if regVol == nil || regVol.HostPath == nil {
		t.Fatal("registration-dir should be hostPath")
	}
	if regVol.HostPath.Path != "/var/lib/kubelet/plugins_registry" {
		t.Errorf("registration-dir path = %q, want /var/lib/kubelet/plugins_registry", regVol.HostPath.Path)
	}
}

// --- CSI Driver Resource Tests ---

func TestCSIDriverResource_GoldenConformance(t *testing.T) {
	cluster := testCluster()
	drv := BuildCSIDriverResource(cluster)

	if drv.Name != blockv1alpha1.CSIDriverName {
		t.Errorf("name = %q, want %s", drv.Name, blockv1alpha1.CSIDriverName)
	}
	if drv.Spec.AttachRequired == nil || *drv.Spec.AttachRequired {
		t.Error("attachRequired should be false")
	}
	if drv.Spec.PodInfoOnMount == nil || *drv.Spec.PodInfoOnMount {
		t.Error("podInfoOnMount should be false")
	}
	if len(drv.Spec.VolumeLifecycleModes) != 1 || drv.Spec.VolumeLifecycleModes[0] != storagev1.VolumeLifecyclePersistent {
		t.Error("volumeLifecycleModes should be [Persistent]")
	}

	// Ownership labels
	if drv.Labels[blockv1alpha1.LabelOwnerNamespace] != "default" {
		t.Error("missing owner-namespace label")
	}
	if drv.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label")
	}
}

// --- RBAC Tests ---

func TestRBAC_GoldenConformance(t *testing.T) {
	cluster := testCluster()

	// ServiceAccount
	sa := BuildServiceAccount(cluster, "kube-system")
	if sa.Name != "sw-block-csi" {
		t.Errorf("SA name = %q, want sw-block-csi", sa.Name)
	}
	if sa.Namespace != "kube-system" {
		t.Errorf("SA namespace = %q, want kube-system", sa.Namespace)
	}

	// ClusterRole
	cr := BuildClusterRole(cluster)
	if cr.Name != "sw-block-csi" {
		t.Errorf("ClusterRole name = %q, want sw-block-csi", cr.Name)
	}

	// Verify key rules match reference
	expectedResources := map[string][]string{
		"persistentvolumes":      {"get", "list", "watch", "create", "delete"},
		"persistentvolumeclaims": {"get", "list", "watch", "update"},
		"storageclasses":         {"get", "list", "watch"},
		"events":                 {"list", "watch", "create", "update", "patch"},
		"csinodes":               {"get", "list", "watch"},
		"nodes":                  {"get", "list", "watch"},
		"volumeattachments":      {"get", "list", "watch"},
	}

	for _, rule := range cr.Rules {
		for _, res := range rule.Resources {
			expected, ok := expectedResources[res]
			if !ok {
				continue
			}
			if len(rule.Verbs) != len(expected) {
				t.Errorf("resource %q: got %d verbs, want %d", res, len(rule.Verbs), len(expected))
			}
			delete(expectedResources, res)
		}
	}
	for res := range expectedResources {
		t.Errorf("missing RBAC rule for resource %q", res)
	}

	// ClusterRoleBinding
	crb := BuildClusterRoleBinding(cluster, "kube-system")
	if crb.Name != "sw-block-csi" {
		t.Errorf("CRB name = %q, want sw-block-csi", crb.Name)
	}
	if len(crb.Subjects) != 1 {
		t.Fatal("expected 1 subject")
	}
	if crb.Subjects[0].Name != "sw-block-csi" || crb.Subjects[0].Namespace != "kube-system" {
		t.Errorf("subject = %+v, want sw-block-csi in kube-system", crb.Subjects[0])
	}
	if crb.RoleRef.Name != "sw-block-csi" {
		t.Errorf("roleRef name = %q, want sw-block-csi", crb.RoleRef.Name)
	}
}

// --- StorageClass Tests ---

func TestStorageClass_GoldenConformance(t *testing.T) {
	cluster := testCluster()
	sc := BuildStorageClass(cluster)

	if sc.Name != "sw-block" {
		t.Errorf("name = %q, want sw-block", sc.Name)
	}
	if sc.Provisioner != blockv1alpha1.CSIDriverName {
		t.Errorf("provisioner = %q, want %s", sc.Provisioner, blockv1alpha1.CSIDriverName)
	}
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		t.Error("volumeBindingMode should be WaitForFirstConsumer")
	}
	if sc.ReclaimPolicy == nil || *sc.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Error("reclaimPolicy should be Delete")
	}

	// Ownership labels
	if sc.Labels[blockv1alpha1.LabelOwnerNamespace] != "default" {
		t.Error("missing owner-namespace label")
	}
}

func TestStorageClass_CustomName(t *testing.T) {
	cluster := testCluster()
	cluster.Spec.StorageClassName = "custom-block"
	sc := BuildStorageClass(cluster)
	if sc.Name != "custom-block" {
		t.Errorf("name = %q, want custom-block", sc.Name)
	}
}

// --- Secret Tests ---

func TestCHAPSecret_Generated(t *testing.T) {
	cluster := testCluster()
	secret, err := BuildCHAPSecret(cluster, "kube-system")
	if err != nil {
		t.Fatalf("BuildCHAPSecret: %v", err)
	}

	if secret.Name != "my-block-chap" {
		t.Errorf("name = %q, want my-block-chap", secret.Name)
	}
	if secret.Namespace != "kube-system" {
		t.Errorf("namespace = %q, want kube-system", secret.Namespace)
	}
	if len(secret.Data["password"]) == 0 {
		t.Error("password should not be empty")
	}
	if string(secret.Data["username"]) != "chap-user" {
		t.Errorf("username = %q, want chap-user", string(secret.Data["username"]))
	}

	// Password should be 64 hex chars (32 bytes)
	if len(secret.Data["password"]) != 64 {
		t.Errorf("password length = %d, want 64 hex chars", len(secret.Data["password"]))
	}

	// Last-rotated annotation should exist
	if _, ok := secret.Annotations[blockv1alpha1.AnnotationLastRotated]; !ok {
		t.Error("missing last-rotated annotation")
	}
}

func TestCHAPSecret_RotationDetection(t *testing.T) {
	cluster := testCluster()

	// No rotation annotation → no rotation needed
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				blockv1alpha1.AnnotationLastRotated: time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	if NeedsRotation(cluster, secret) {
		t.Error("should not need rotation without annotation")
	}

	// Set rotation annotation in the future
	cluster.Annotations = map[string]string{
		blockv1alpha1.AnnotationRotateSecret: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
	if !NeedsRotation(cluster, secret) {
		t.Error("should need rotation when annotation is newer")
	}

	// Set rotation annotation in the past
	cluster.Annotations[blockv1alpha1.AnnotationRotateSecret] = time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	if NeedsRotation(cluster, secret) {
		t.Error("should not need rotation when annotation is older")
	}
}

func TestCHAPSecret_Regeneration(t *testing.T) {
	cluster := testCluster()
	secret, _ := BuildCHAPSecret(cluster, "kube-system")
	oldPassword := string(secret.Data["password"])

	if err := RegenerateCHAPPassword(secret); err != nil {
		t.Fatalf("RegenerateCHAPPassword: %v", err)
	}

	newPassword := string(secret.Data["password"])
	if newPassword == oldPassword {
		t.Error("password should change after regeneration")
	}
	if len(newPassword) != 64 {
		t.Errorf("new password length = %d, want 64", len(newPassword))
	}
}

// --- Ownership Tests ---

func TestOwnership_CheckVariants(t *testing.T) {
	cluster := testCluster()

	// Owned by this CR
	owned := &metav1.ObjectMeta{
		Labels: map[string]string{
			blockv1alpha1.LabelOwnerNamespace: "default",
			blockv1alpha1.LabelOwnerName:      "my-block",
		},
	}
	if CheckOwnership(owned, cluster) != OwnershipOwned {
		t.Error("should be OwnershipOwned")
	}

	// Owned by different CR
	conflict := &metav1.ObjectMeta{
		Labels: map[string]string{
			blockv1alpha1.LabelOwnerNamespace: "other-ns",
			blockv1alpha1.LabelOwnerName:      "other-block",
		},
	}
	if CheckOwnership(conflict, cluster) != OwnershipConflict {
		t.Error("should be OwnershipConflict")
	}

	// No labels
	orphan := &metav1.ObjectMeta{}
	if CheckOwnership(orphan, cluster) != OwnershipOrphan {
		t.Error("should be OwnershipOrphan")
	}

	// Labels present but no owner labels
	noOwner := &metav1.ObjectMeta{
		Labels: map[string]string{"foo": "bar"},
	}
	if CheckOwnership(noOwner, cluster) != OwnershipOrphan {
		t.Error("should be OwnershipOrphan for labels without owner keys")
	}
}

func TestConflictOwner(t *testing.T) {
	obj := &metav1.ObjectMeta{
		Labels: map[string]string{
			blockv1alpha1.LabelOwnerNamespace: "prod",
			blockv1alpha1.LabelOwnerName:      "block-1",
		},
	}
	result := ConflictOwner(obj)
	if result != "prod/block-1" {
		t.Errorf("ConflictOwner = %q, want prod/block-1", result)
	}
}

// --- Labels Tests ---

func TestLabels_CommonAndComponent(t *testing.T) {
	cluster := testCluster()

	common := CommonLabels(cluster)
	if common[labelApp] != "sw-block" {
		t.Error("missing app label")
	}
	if common[labelInstance] != "my-block" {
		t.Error("missing instance label")
	}
	if common[labelManagedBy] != "sw-block-operator" {
		t.Error("missing managed-by label")
	}

	comp := ComponentLabels(cluster, "master")
	if comp[labelComponent] != "master" {
		t.Error("missing component label")
	}

	sel := SelectorLabels(cluster, "csi-node")
	if len(sel) != 3 {
		t.Errorf("selector labels count = %d, want 3", len(sel))
	}
}

// --- CSI Controller cross-NS ownership test ---

func TestCSIController_CrossNamespaceOwnership(t *testing.T) {
	cluster := testCluster()
	dep := BuildCSIControllerDeployment(cluster, "master:9333", "kube-system")

	// Cross-namespace resources must NOT have ownerRef (k8s GC ignores them)
	if len(dep.OwnerReferences) != 0 {
		t.Errorf("cross-namespace resources should not have ownerRef, got %d", len(dep.OwnerReferences))
	}

	// Must have ownership labels for finalizer cleanup
	if dep.Labels[blockv1alpha1.LabelOwnerNamespace] != "default" {
		t.Error("missing owner-namespace label")
	}
	if dep.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label")
	}
}

// --- Same-namespace resources DO get ownerRef ---

func TestMasterStatefulSet_HasOwnerRef(t *testing.T) {
	cluster := fullStackCluster()
	sts := BuildMasterStatefulSet(cluster)
	if len(sts.OwnerReferences) != 1 {
		t.Fatalf("same-namespace resources should have ownerRef, got %d", len(sts.OwnerReferences))
	}
	if sts.OwnerReferences[0].Kind != "SeaweedBlockCluster" {
		t.Errorf("ownerRef kind = %q", sts.OwnerReferences[0].Kind)
	}
}

// --- CSI Node cross-NS ownership ---

func TestCSINode_CrossNamespaceOwnership(t *testing.T) {
	cluster := testCluster()
	ds := BuildCSINodeDaemonSet(cluster, "kube-system")

	if len(ds.OwnerReferences) != 0 {
		t.Errorf("CSI node should not have ownerRef (cross-namespace), got %d", len(ds.OwnerReferences))
	}
	if ds.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label on CSI node")
	}
}

// --- ServiceAccount cross-NS ownership ---

func TestServiceAccount_CrossNamespaceOwnership(t *testing.T) {
	cluster := testCluster()
	sa := BuildServiceAccount(cluster, "kube-system")

	if len(sa.OwnerReferences) != 0 {
		t.Errorf("SA should not have ownerRef (cross-namespace), got %d", len(sa.OwnerReferences))
	}
	if sa.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label on SA")
	}
}

// --- CHAP Secret cross-NS ownership ---

func TestCHAPSecret_CrossNamespaceOwnership(t *testing.T) {
	cluster := testCluster()
	secret, err := BuildCHAPSecret(cluster, "kube-system")
	if err != nil {
		t.Fatal(err)
	}

	if len(secret.OwnerReferences) != 0 {
		t.Errorf("CHAP secret should not have ownerRef (cross-namespace), got %d", len(secret.OwnerReferences))
	}
	if secret.Labels[blockv1alpha1.LabelOwnerName] != "my-block" {
		t.Error("missing owner-name label on CHAP secret")
	}
}

// --- Conformance: all builders produce labeled output ---

func TestAllBuilders_HaveLabels(t *testing.T) {
	cluster := testCluster()

	checks := []struct {
		name   string
		labels map[string]string
	}{
		{"CSIController", BuildCSIControllerDeployment(cluster, "m:9333", "kube-system").Labels},
		{"CSINode", BuildCSINodeDaemonSet(cluster, "kube-system").Labels},
		{"CSIDriver", BuildCSIDriverResource(cluster).Labels},
		{"ClusterRole", BuildClusterRole(cluster).Labels},
		{"CRB", BuildClusterRoleBinding(cluster, "kube-system").Labels},
		{"StorageClass", BuildStorageClass(cluster).Labels},
		{"ServiceAccount", BuildServiceAccount(cluster, "kube-system").Labels},
	}

	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.labels[labelApp] != "sw-block" {
				t.Errorf("%s: missing app label", c.name)
			}
			if c.labels[labelManagedBy] != "sw-block-operator" {
				t.Errorf("%s: missing managed-by label", c.name)
			}
		})
	}
}

// --- Type assertion: CSI node must produce DaemonSet ---

func TestCSINode_IsDaemonSet(t *testing.T) {
	cluster := testCluster()
	ds := BuildCSINodeDaemonSet(cluster, "kube-system")
	var _ *appsv1.DaemonSet = ds // compile-time type check
}

func TestCSIController_IsDeployment(t *testing.T) {
	cluster := testCluster()
	dep := BuildCSIControllerDeployment(cluster, "m:9333", "kube-system")
	var _ *appsv1.Deployment = dep // compile-time type check
}

// --- Full-Stack: Master Builder Tests ---

func fullStackCluster() *blockv1alpha1.SeaweedBlockCluster {
	replicas := int32(1)
	return &blockv1alpha1.SeaweedBlockCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-block",
			Namespace: "default",
			UID:       "test-uid-456",
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
			StorageClassName: "sw-block",
			CSINamespace:     "kube-system",
		},
	}
}

func TestMasterService_GoldenConformance(t *testing.T) {
	cluster := fullStackCluster()
	svc := BuildMasterService(cluster)

	if svc.Name != "my-block-master" {
		t.Errorf("name = %q, want my-block-master", svc.Name)
	}
	if svc.Namespace != "default" {
		t.Errorf("namespace = %q, want default", svc.Namespace)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Error("should be headless (ClusterIP=None)")
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("publishNotReadyAddresses should be true")
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("ports = %d, want 2", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != 9333 {
		t.Errorf("http port = %d, want 9333", svc.Spec.Ports[0].Port)
	}
	if svc.Spec.Ports[1].Port != 19333 {
		t.Errorf("grpc port = %d, want 19333", svc.Spec.Ports[1].Port)
	}
}

func TestMasterStatefulSet_GoldenConformance(t *testing.T) {
	cluster := fullStackCluster()
	sts := BuildMasterStatefulSet(cluster)

	if sts.Name != "my-block-master" {
		t.Errorf("name = %q, want my-block-master", sts.Name)
	}
	if sts.Spec.ServiceName != "my-block-master" {
		t.Errorf("serviceName = %q, want my-block-master", sts.Spec.ServiceName)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 1 {
		t.Error("replicas should be 1")
	}

	// Container
	if len(sts.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(sts.Spec.Template.Spec.Containers))
	}
	c := sts.Spec.Template.Spec.Containers[0]
	if c.Name != "master" {
		t.Errorf("container name = %q", c.Name)
	}
	if c.Image != "chrislusf/seaweedfs:latest" {
		t.Errorf("image = %q", c.Image)
	}
	if len(c.Command) != 1 || c.Command[0] != "/usr/bin/weed" {
		t.Errorf("command = %v", c.Command)
	}

	// Key args
	if !containsArg(c.Args, "master") {
		t.Error("missing 'master' arg")
	}
	if !containsArg(c.Args, "-port=9333") {
		t.Error("missing -port arg")
	}
	if !containsArg(c.Args, "-mdir=/data") {
		t.Error("missing -mdir arg")
	}
	if !containsArg(c.Args, "-ip.bind=0.0.0.0") {
		t.Error("missing -ip.bind arg")
	}

	// Readiness probe
	if c.ReadinessProbe == nil {
		t.Fatal("missing readiness probe")
	}
	if c.ReadinessProbe.HTTPGet.Path != "/cluster/status" {
		t.Errorf("readiness path = %q, want /cluster/status", c.ReadinessProbe.HTTPGet.Path)
	}

	// VolumeClaimTemplates
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("VCTs = %d, want 1", len(sts.Spec.VolumeClaimTemplates))
	}
	if sts.Spec.VolumeClaimTemplates[0].Name != "data" {
		t.Error("VCT name should be 'data'")
	}

	// Volume mount
	dataMount := findVolumeMount(c.VolumeMounts, "data")
	if dataMount == nil || dataMount.MountPath != "/data" {
		t.Error("missing /data mount")
	}
}

func TestMasterStatefulSet_EmptyDir_NoStorage(t *testing.T) {
	cluster := fullStackCluster()
	cluster.Spec.Master.Storage = nil
	sts := BuildMasterStatefulSet(cluster)

	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Error("should not have VCTs when no storage specified")
	}
	vol := findVolume(sts.Spec.Template.Spec.Volumes, "data")
	if vol == nil || vol.EmptyDir == nil {
		t.Error("should use emptyDir when no storage specified")
	}
}

// --- Full-Stack: Volume Builder Tests ---

func TestVolumeService_GoldenConformance(t *testing.T) {
	cluster := fullStackCluster()
	svc := BuildVolumeService(cluster)

	if svc.Name != "my-block-volume" {
		t.Errorf("name = %q, want my-block-volume", svc.Name)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Error("should be headless")
	}
	if len(svc.Spec.Ports) != 3 {
		t.Fatalf("ports = %d, want 3 (http, grpc, iscsi)", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("http port = %d", svc.Spec.Ports[0].Port)
	}
	if svc.Spec.Ports[1].Port != 18080 {
		t.Errorf("grpc port = %d", svc.Spec.Ports[1].Port)
	}
	if svc.Spec.Ports[2].Port != 3260 {
		t.Errorf("iscsi port = %d", svc.Spec.Ports[2].Port)
	}
}

func TestVolumeStatefulSet_GoldenConformance(t *testing.T) {
	cluster := fullStackCluster()
	masterAddr := "my-block-master.default:9333"
	sts := BuildVolumeStatefulSet(cluster, masterAddr)

	if sts.Name != "my-block-volume" {
		t.Errorf("name = %q, want my-block-volume", sts.Name)
	}
	if sts.Spec.ServiceName != "my-block-volume" {
		t.Errorf("serviceName = %q", sts.Spec.ServiceName)
	}

	c := sts.Spec.Template.Spec.Containers[0]
	if c.Name != "volume" {
		t.Errorf("container name = %q", c.Name)
	}
	if c.Image != "chrislusf/seaweedfs:latest" {
		t.Errorf("image = %q", c.Image)
	}

	// Key args
	if !containsArg(c.Args, "volume") {
		t.Error("missing 'volume' arg")
	}
	if !containsArg(c.Args, "-port=8080") {
		t.Error("missing -port arg")
	}
	if !containsArg(c.Args, "-dir=/data1") {
		t.Error("missing -dir arg")
	}
	if !containsArg(c.Args, "-master=my-block-master.default:9333") {
		t.Error("missing -master arg")
	}
	if !containsArg(c.Args, "-block.dir=/data1/block") {
		t.Error("missing -block.dir arg")
	}
	if !containsArg(c.Args, "-block.listen=0.0.0.0:3260") {
		t.Error("missing -block.listen arg")
	}

	// Default portal: $(POD_IP):3260,1
	if !containsArg(c.Args, "-block.portal=$(POD_IP):3260,1") {
		t.Error("missing default -block.portal arg")
	}

	// POD_IP env var
	if len(c.Env) < 1 || c.Env[0].Name != "POD_IP" {
		t.Error("missing POD_IP env var")
	}

	// Ports
	if len(c.Ports) != 3 {
		t.Fatalf("ports = %d, want 3", len(c.Ports))
	}

	// VolumeClaimTemplates
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("VCTs = %d, want 1", len(sts.Spec.VolumeClaimTemplates))
	}

	// Volume mount
	dataMount := findVolumeMount(c.VolumeMounts, "data")
	if dataMount == nil || dataMount.MountPath != "/data1" {
		t.Error("missing /data1 mount")
	}
}

func TestVolumeStatefulSet_PortalOverride(t *testing.T) {
	cluster := fullStackCluster()
	cluster.Spec.Volume.PortalOverride = "192.168.1.100:3260,1"
	sts := BuildVolumeStatefulSet(cluster, "master:9333")

	c := sts.Spec.Template.Spec.Containers[0]
	if !containsArg(c.Args, "-block.portal=192.168.1.100:3260,1") {
		t.Error("portal override not applied")
	}
}

func TestVolumeStatefulSet_ExtraArgs(t *testing.T) {
	cluster := fullStackCluster()
	cluster.Spec.Volume.ExtraArgs = []string{"-compactionMBps=50"}
	sts := BuildVolumeStatefulSet(cluster, "master:9333")

	c := sts.Spec.Template.Spec.Containers[0]
	if !containsArg(c.Args, "-compactionMBps=50") {
		t.Error("extraArgs not passed through")
	}
}

func TestMasterStatefulSet_ExtraArgs(t *testing.T) {
	cluster := fullStackCluster()
	cluster.Spec.Master.ExtraArgs = []string{"-defaultReplication=001"}
	sts := BuildMasterStatefulSet(cluster)

	c := sts.Spec.Template.Spec.Containers[0]
	if !containsArg(c.Args, "-defaultReplication=001") {
		t.Error("extraArgs not passed through")
	}
}

// --- M1 fix: Volume readiness probe ---

func TestVolumeStatefulSet_HasReadinessProbe(t *testing.T) {
	cluster := fullStackCluster()
	sts := BuildVolumeStatefulSet(cluster, "master:9333")
	c := sts.Spec.Template.Spec.Containers[0]

	if c.ReadinessProbe == nil {
		t.Fatal("volume container missing readiness probe")
	}
	if c.ReadinessProbe.HTTPGet == nil {
		t.Fatal("volume readiness probe should be HTTPGet")
	}
	if c.ReadinessProbe.HTTPGet.Path != "/status" {
		t.Errorf("readiness probe path = %q, want /status", c.ReadinessProbe.HTTPGet.Path)
	}
}

// --- M3 fix: PVC StorageClassName wired ---

func TestMasterStatefulSet_PVCStorageClassName(t *testing.T) {
	cluster := fullStackCluster()
	sc := "fast-ssd"
	cluster.Spec.Master.Storage.StorageClassName = &sc
	sts := BuildMasterStatefulSet(cluster)

	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatal("expected 1 VCT")
	}
	pvc := sts.Spec.VolumeClaimTemplates[0]
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast-ssd" {
		t.Errorf("PVC storageClassName = %v, want fast-ssd", pvc.Spec.StorageClassName)
	}
}

func TestVolumeStatefulSet_PVCStorageClassName(t *testing.T) {
	cluster := fullStackCluster()
	sc := "slow-hdd"
	cluster.Spec.Volume.Storage.StorageClassName = &sc
	sts := BuildVolumeStatefulSet(cluster, "master:9333")

	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatal("expected 1 VCT")
	}
	pvc := sts.Spec.VolumeClaimTemplates[0]
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "slow-hdd" {
		t.Errorf("PVC storageClassName = %v, want slow-hdd", pvc.Spec.StorageClassName)
	}
}
