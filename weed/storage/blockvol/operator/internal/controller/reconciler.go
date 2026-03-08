package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/internal/resources"
)

// Reconciler reconciles a SeaweedBlockCluster object.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=block.seaweedfs.com,resources=seaweedblockclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=block.seaweedfs.com,resources=seaweedblockclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=block.seaweedfs.com,resources=seaweedblockclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;daemonsets;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=csidrivers;storageclasses,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles a single reconciliation loop for a SeaweedBlockCluster.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch CR
	var cluster blockv1alpha1.SeaweedBlockCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Handle finalizer for resource cleanup (cluster-scoped + cross-namespace)
	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, blockv1alpha1.FinalizerName) {
			if err := r.cleanupOwnedResources(ctx, &cluster); err != nil {
				logger.Error(err, "failed to cleanup owned resources")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&cluster, blockv1alpha1.FinalizerName)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(&cluster, blockv1alpha1.FinalizerName) {
		controllerutil.AddFinalizer(&cluster, blockv1alpha1.FinalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 3. Apply defaults
	applyDefaults(&cluster.Spec)

	// 4. Validate
	if err := validateName(cluster.Name); err != nil {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionValidationFailed, "ValidationFailed", err.Error())
	}
	if err := validate(&cluster.Spec); err != nil {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionValidationFailed, "ValidationFailed", err.Error())
	}

	// 5. Determine mode and master address
	var masterAddr string
	if cluster.Spec.Master != nil {
		// Full-stack mode — use FQDN for robustness (L1 fix)
		masterAddr = fmt.Sprintf("%s-master.%s.svc.cluster.local:%d",
			cluster.Name, cluster.Namespace, cluster.Spec.Master.Port)

		// 6a. Master StatefulSet + Service
		if err := r.reconcileMasterService(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileMasterStatefulSet(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}

		// 6b. Volume StatefulSet + Service (auto-create VolumeSpec if nil)
		if cluster.Spec.Volume == nil {
			cluster.Spec.Volume = &blockv1alpha1.VolumeSpec{}
			applyDefaults(&cluster.Spec)
		}
		if err := r.reconcileVolumeService(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileVolumeStatefulSet(ctx, &cluster, masterAddr); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// CSI-only mode
		masterAddr = cluster.Spec.MasterRef.Address
	}

	csiNS := cluster.Spec.CSINamespace

	// 8. CHAP Secret
	if err := r.reconcileCHAPSecret(ctx, &cluster, csiNS); err != nil {
		return ctrl.Result{}, err
	}

	// 9-11. RBAC
	if err := r.reconcileServiceAccount(ctx, &cluster, csiNS); err != nil {
		return ctrl.Result{}, err
	}
	if conflict, err := r.reconcileClusterRole(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	} else if conflict != "" {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionResourceConflict, "ResourceConflict", conflict)
	}
	if conflict, err := r.reconcileClusterRoleBinding(ctx, &cluster, csiNS); err != nil {
		return ctrl.Result{}, err
	} else if conflict != "" {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionResourceConflict, "ResourceConflict", conflict)
	}

	// 12. CSIDriver
	if conflict, err := r.reconcileCSIDriver(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	} else if conflict != "" {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionResourceConflict, "ResourceConflict", conflict)
	}

	// 13. CSI Controller Deployment
	if err := r.reconcileCSIController(ctx, &cluster, masterAddr, csiNS); err != nil {
		return ctrl.Result{}, err
	}

	// 14. CSI Node DaemonSet
	if err := r.reconcileCSINode(ctx, &cluster, csiNS); err != nil {
		return ctrl.Result{}, err
	}

	// 15. StorageClass
	if conflict, err := r.reconcileStorageClass(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	} else if conflict != "" {
		return r.setFailedStatus(ctx, &cluster, blockv1alpha1.ConditionResourceConflict, "ResourceConflict", conflict)
	}

	// 16-17. Compute and update status
	return r.updateStatus(ctx, &cluster, masterAddr)
}

// SetupWithManager registers the controller with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockv1alpha1.SeaweedBlockCluster{}).
		// Owns() only works same-namespace. CSI resources in csiNamespace use
		// label-based ownership + finalizer cleanup instead. We still watch
		// same-namespace resources (master/volume StatefulSets and Services).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

// --- Namespace-scoped reconcilers (cross-namespace CSI resources) ---

func (r *Reconciler) reconcileServiceAccount(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) error {
	desired := resources.BuildServiceAccount(cluster, csiNS)
	return r.createOrUpdateCrossNamespace(ctx, cluster, desired, &corev1.ServiceAccount{})
}

func (r *Reconciler) reconcileCSIController(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, masterAddr, csiNS string) error {
	desired := resources.BuildCSIControllerDeployment(cluster, masterAddr, csiNS)
	return r.createOrUpdateCrossNamespace(ctx, cluster, desired, &appsv1.Deployment{})
}

func (r *Reconciler) reconcileCSINode(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) error {
	desired := resources.BuildCSINodeDaemonSet(cluster, csiNS)
	return r.createOrUpdateCrossNamespace(ctx, cluster, desired, &appsv1.DaemonSet{})
}

func (r *Reconciler) reconcileCHAPSecret(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) error {
	// If user provides their own secret, skip
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.SecretRef != nil {
		return nil
	}

	secretName := resources.CHAPSecretName(cluster)
	var existing corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: csiNS}, &existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Create new secret
		secret, genErr := resources.BuildCHAPSecret(cluster, csiNS)
		if genErr != nil {
			return genErr
		}
		return r.Create(ctx, secret)
	}

	// Secret exists — check rotation
	if resources.NeedsRotation(cluster, &existing) {
		if err := resources.RegenerateCHAPPassword(&existing); err != nil {
			return err
		}
		return r.Update(ctx, &existing)
	}

	return nil
}

// --- Namespace-scoped reconcilers (same-namespace, use ownerRef) ---

func (r *Reconciler) reconcileMasterService(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) error {
	desired := resources.BuildMasterService(cluster)
	return r.createOrUpdateSameNamespace(ctx, desired, &corev1.Service{})
}

func (r *Reconciler) reconcileMasterStatefulSet(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) error {
	desired := resources.BuildMasterStatefulSet(cluster)
	return r.createOrUpdateSameNamespace(ctx, desired, &appsv1.StatefulSet{})
}

func (r *Reconciler) reconcileVolumeService(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) error {
	desired := resources.BuildVolumeService(cluster)
	return r.createOrUpdateSameNamespace(ctx, desired, &corev1.Service{})
}

func (r *Reconciler) reconcileVolumeStatefulSet(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, masterAddr string) error {
	desired := resources.BuildVolumeStatefulSet(cluster, masterAddr)
	return r.createOrUpdateSameNamespace(ctx, desired, &appsv1.StatefulSet{})
}

// --- Cluster-scoped reconcilers ---

func (r *Reconciler) reconcileClusterRole(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) (conflict string, err error) {
	desired := resources.BuildClusterRole(cluster)
	return r.createOrUpdateClusterScoped(ctx, cluster, desired, &rbacv1.ClusterRole{})
}

func (r *Reconciler) reconcileClusterRoleBinding(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) (conflict string, err error) {
	desired := resources.BuildClusterRoleBinding(cluster, csiNS)
	return r.createOrUpdateClusterScoped(ctx, cluster, desired, &rbacv1.ClusterRoleBinding{})
}

func (r *Reconciler) reconcileCSIDriver(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) (conflict string, err error) {
	desired := resources.BuildCSIDriverResource(cluster)
	return r.createOrUpdateClusterScoped(ctx, cluster, desired, &storagev1.CSIDriver{})
}

func (r *Reconciler) reconcileStorageClass(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) (conflict string, err error) {
	desired := resources.BuildStorageClass(cluster)

	var existing storagev1.StorageClass
	err = r.Get(ctx, types.NamespacedName{Name: desired.Name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", r.Create(ctx, desired)
		}
		return "", err
	}

	ownership := resources.CheckOwnership(&existing, cluster)
	switch ownership {
	case resources.OwnershipOwned:
		existing.Labels = desired.Labels
		return "", r.Update(ctx, &existing)
	case resources.OwnershipConflict:
		return fmt.Sprintf("StorageClass %s already owned by %s", desired.Name, resources.ConflictOwner(&existing)), nil
	case resources.OwnershipOrphan:
		if cluster.Spec.AdoptExistingStorageClass {
			existing.Labels = desired.Labels
			return "", r.Update(ctx, &existing)
		}
		return fmt.Sprintf("StorageClass %s exists and is not managed by this operator. Set adoptExistingStorageClass: true to adopt.", desired.Name), nil
	}
	return "", nil
}

// --- Generic helpers ---

// createOrUpdateSameNamespace creates or updates a resource in the same namespace as the CR.
// Uses ownerReference for GC.
func (r *Reconciler) createOrUpdateSameNamespace(ctx context.Context, desired client.Object, existing client.Object) error {
	key := types.NamespacedName{
		Name:      desired.GetName(),
		Namespace: desired.GetNamespace(),
	}

	err := r.Get(ctx, key, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, desired)
		}
		return err
	}

	desired.SetResourceVersion(existing.GetResourceVersion())
	desired.SetUID(existing.GetUID())
	return r.Update(ctx, desired)
}

// createOrUpdateCrossNamespace creates or updates a resource in a different namespace than the CR.
// Uses ownership labels (not ownerRef) for tracking. Cleanup happens via finalizer.
func (r *Reconciler) createOrUpdateCrossNamespace(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, desired client.Object, existing client.Object) error {
	key := types.NamespacedName{
		Name:      desired.GetName(),
		Namespace: desired.GetNamespace(),
	}

	err := r.Get(ctx, key, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, desired)
		}
		return err
	}

	// On update, ensure ownership labels are preserved
	resources.SetCrossNamespaceOwnership(cluster, desired)
	desired.SetResourceVersion(existing.GetResourceVersion())
	desired.SetUID(existing.GetUID())
	return r.Update(ctx, desired)
}

// createOrUpdateClusterScoped creates or updates a cluster-scoped resource with ownership labels.
// Returns (conflict message, error). Non-empty conflict means another CR owns this resource.
func (r *Reconciler) createOrUpdateClusterScoped(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, desired client.Object, existing client.Object) (string, error) {
	key := types.NamespacedName{Name: desired.GetName()}

	err := r.Get(ctx, key, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", r.Create(ctx, desired)
		}
		return "", err
	}

	ownership := resources.CheckOwnership(existing, cluster)
	switch ownership {
	case resources.OwnershipOwned:
		desired.SetResourceVersion(existing.GetResourceVersion())
		desired.SetUID(existing.GetUID())
		return "", r.Update(ctx, desired)
	case resources.OwnershipConflict:
		kind := existing.GetObjectKind().GroupVersionKind().Kind
		if kind == "" {
			kind = fmt.Sprintf("%T", existing)
		}
		return fmt.Sprintf("%s %s already owned by %s", kind, desired.GetName(), resources.ConflictOwner(existing)), nil
	case resources.OwnershipOrphan:
		// M4 fix: require explicit opt-in for orphan adoption of security-sensitive resources.
		// For CSIDriver/ClusterRole/CRB, fail rather than silently adopting.
		kind := existing.GetObjectKind().GroupVersionKind().Kind
		if kind == "" {
			kind = fmt.Sprintf("%T", existing)
		}
		return fmt.Sprintf("%s %s exists but is not managed by this operator", kind, desired.GetName()), nil
	}
	return "", nil
}

// cleanupOwnedResources deletes all resources owned by this CR:
// cluster-scoped (CSIDriver, ClusterRole, CRB, StorageClass) AND
// cross-namespace (CSI controller, node, SA, secret in csiNamespace).
func (r *Reconciler) cleanupOwnedResources(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster) error {
	logger := log.FromContext(ctx)

	// --- Cluster-scoped resources ---

	// CSIDriver
	var csiDriver storagev1.CSIDriver
	if err := r.Get(ctx, types.NamespacedName{Name: blockv1alpha1.CSIDriverName}, &csiDriver); err == nil {
		if resources.IsOwnedBy(&csiDriver, cluster) {
			logger.Info("deleting CSIDriver", "name", csiDriver.Name)
			if err := r.Delete(ctx, &csiDriver); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// ClusterRole
	var cr rbacv1.ClusterRole
	if err := r.Get(ctx, types.NamespacedName{Name: resources.ClusterRoleName()}, &cr); err == nil {
		if resources.IsOwnedBy(&cr, cluster) {
			logger.Info("deleting ClusterRole", "name", cr.Name)
			if err := r.Delete(ctx, &cr); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// ClusterRoleBinding
	var crb rbacv1.ClusterRoleBinding
	if err := r.Get(ctx, types.NamespacedName{Name: resources.ClusterRoleBindingName()}, &crb); err == nil {
		if resources.IsOwnedBy(&crb, cluster) {
			logger.Info("deleting ClusterRoleBinding", "name", crb.Name)
			if err := r.Delete(ctx, &crb); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// StorageClass
	scName := cluster.Spec.StorageClassName
	if scName == "" {
		scName = blockv1alpha1.DefaultStorageClassName
	}
	var sc storagev1.StorageClass
	if err := r.Get(ctx, types.NamespacedName{Name: scName}, &sc); err == nil {
		if resources.IsOwnedBy(&sc, cluster) {
			logger.Info("deleting StorageClass", "name", sc.Name)
			if err := r.Delete(ctx, &sc); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// --- Cross-namespace resources in csiNamespace (H1 fix) ---
	csiNS := cluster.Spec.CSINamespace
	if csiNS == "" {
		csiNS = blockv1alpha1.DefaultCSINamespace
	}

	// CSI Controller Deployment
	var dep appsv1.Deployment
	depName := resources.CSIControllerName(cluster)
	if err := r.Get(ctx, types.NamespacedName{Name: depName, Namespace: csiNS}, &dep); err == nil {
		if resources.IsOwnedBy(&dep, cluster) {
			logger.Info("deleting CSI controller Deployment", "name", depName, "namespace", csiNS)
			if err := r.Delete(ctx, &dep); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// CSI Node DaemonSet
	var ds appsv1.DaemonSet
	dsName := resources.CSINodeName(cluster)
	if err := r.Get(ctx, types.NamespacedName{Name: dsName, Namespace: csiNS}, &ds); err == nil {
		if resources.IsOwnedBy(&ds, cluster) {
			logger.Info("deleting CSI node DaemonSet", "name", dsName, "namespace", csiNS)
			if err := r.Delete(ctx, &ds); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// ServiceAccount
	var sa corev1.ServiceAccount
	saName := resources.ServiceAccountName()
	if err := r.Get(ctx, types.NamespacedName{Name: saName, Namespace: csiNS}, &sa); err == nil {
		if resources.IsOwnedBy(&sa, cluster) {
			logger.Info("deleting ServiceAccount", "name", saName, "namespace", csiNS)
			if err := r.Delete(ctx, &sa); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	// CHAP Secret
	var secret corev1.Secret
	secretName := resources.CHAPSecretName(cluster)
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: csiNS}, &secret); err == nil {
		if resources.IsOwnedBy(&secret, cluster) {
			logger.Info("deleting CHAP Secret", "name", secretName, "namespace", csiNS)
			if err := r.Delete(ctx, &secret); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

// --- Status helpers ---

func (r *Reconciler) setFailedStatus(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, condType, reason, message string) (ctrl.Result, error) {
	cluster.Status.Phase = blockv1alpha1.PhaseFailed
	setCondition(&cluster.Status, metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})

	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) updateStatus(ctx context.Context, cluster *blockv1alpha1.SeaweedBlockCluster, masterAddr string) (ctrl.Result, error) {
	cluster.Status.MasterAddress = masterAddr
	cluster.Status.Phase = blockv1alpha1.PhaseRunning

	// Clear any previous failure conditions
	removeCondition(&cluster.Status, blockv1alpha1.ConditionResourceConflict)
	removeCondition(&cluster.Status, blockv1alpha1.ConditionValidationFailed)

	// MasterReady condition
	if cluster.Spec.Master != nil {
		masterReady := r.checkStatefulSetReady(ctx, cluster.Name+"-master", cluster.Namespace)
		setCondition(&cluster.Status, metav1.Condition{
			Type:               blockv1alpha1.ConditionMasterReady,
			Status:             boolToStatus(masterReady),
			Reason:             readyReason(masterReady),
			LastTransitionTime: metav1.Now(),
		})
		if !masterReady {
			cluster.Status.Phase = blockv1alpha1.PhasePending
		}
	} else {
		setCondition(&cluster.Status, metav1.Condition{
			Type:               blockv1alpha1.ConditionMasterReady,
			Status:             metav1.ConditionTrue,
			Reason:             "ExternalMaster",
			Message:            "Using external master: " + masterAddr,
			LastTransitionTime: metav1.Now(),
		})
	}

	// VolumeReady condition
	if cluster.Spec.Volume != nil && cluster.Spec.Master != nil {
		volReady := r.checkStatefulSetReady(ctx, cluster.Name+"-volume", cluster.Namespace)
		setCondition(&cluster.Status, metav1.Condition{
			Type:               blockv1alpha1.ConditionVolumeReady,
			Status:             boolToStatus(volReady),
			Reason:             readyReason(volReady),
			LastTransitionTime: metav1.Now(),
		})
		if !volReady {
			cluster.Status.Phase = blockv1alpha1.PhasePending
		}
	} else {
		setCondition(&cluster.Status, metav1.Condition{
			Type:               blockv1alpha1.ConditionVolumeReady,
			Status:             metav1.ConditionTrue,
			Reason:             "ExternalVolume",
			LastTransitionTime: metav1.Now(),
		})
	}

	// CSIReady condition
	csiNS := cluster.Spec.CSINamespace
	if csiNS == "" {
		csiNS = blockv1alpha1.DefaultCSINamespace
	}
	controllerReady := r.checkDeploymentReady(ctx, resources.CSIControllerName(cluster), csiNS)
	nodeReady := r.checkDaemonSetReady(ctx, resources.CSINodeName(cluster), csiNS)
	csiReady := controllerReady && nodeReady
	setCondition(&cluster.Status, metav1.Condition{
		Type:               blockv1alpha1.ConditionCSIReady,
		Status:             boolToStatus(csiReady),
		Reason:             readyReason(csiReady),
		LastTransitionTime: metav1.Now(),
	})

	// AuthConfigured condition
	authOK := cluster.Spec.Auth != nil && cluster.Spec.Auth.SecretRef != nil
	if !authOK {
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{
			Name:      resources.CHAPSecretName(cluster),
			Namespace: csiNS,
		}, &secret); err == nil {
			authOK = true
		}
	}
	setCondition(&cluster.Status, metav1.Condition{
		Type:               blockv1alpha1.ConditionAuthConfigured,
		Status:             boolToStatus(authOK),
		Reason:             readyReason(authOK),
		LastTransitionTime: metav1.Now(),
	})

	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) checkStatefulSetReady(ctx context.Context, name, namespace string) bool {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &sts); err != nil {
		return false
	}
	return sts.Status.ReadyReplicas >= 1
}

func (r *Reconciler) checkDeploymentReady(ctx context.Context, name, namespace string) bool {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &dep); err != nil {
		return false
	}
	return dep.Status.ReadyReplicas >= 1
}

func (r *Reconciler) checkDaemonSetReady(ctx context.Context, name, namespace string) bool {
	var ds appsv1.DaemonSet
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &ds); err != nil {
		return false
	}
	return ds.Status.NumberReady >= 1
}

// --- Condition helpers ---

func setCondition(status *blockv1alpha1.SeaweedBlockClusterStatus, cond metav1.Condition) {
	for i, existing := range status.Conditions {
		if existing.Type == cond.Type {
			status.Conditions[i] = cond
			return
		}
	}
	status.Conditions = append(status.Conditions, cond)
}

func removeCondition(status *blockv1alpha1.SeaweedBlockClusterStatus, condType string) {
	filtered := status.Conditions[:0]
	for _, c := range status.Conditions {
		if c.Type != condType {
			filtered = append(filtered, c)
		}
	}
	status.Conditions = filtered
}

func boolToStatus(b bool) metav1.ConditionStatus {
	if b {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func readyReason(ready bool) string {
	if ready {
		return "Ready"
	}
	return "NotReady"
}
