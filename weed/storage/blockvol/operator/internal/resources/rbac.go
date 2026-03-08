package resources

import (
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

const (
	clusterRoleName    = "sw-block-csi"
	clusterRoleBinding = "sw-block-csi"
	serviceAccountName = "sw-block-csi"
)

// ServiceAccountName returns the fixed CSI service account name.
func ServiceAccountName() string { return serviceAccountName }

// ClusterRoleName returns the fixed CSI ClusterRole name.
func ClusterRoleName() string { return clusterRoleName }

// ClusterRoleBindingName returns the fixed CSI ClusterRoleBinding name.
func ClusterRoleBindingName() string { return clusterRoleBinding }

// BuildServiceAccount constructs the CSI ServiceAccount.
// Reference: csi/deploy/rbac.yaml
func BuildServiceAccount(cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceAccountName,
			Namespace: csiNS,
			Labels:    ComponentLabels(cluster, "csi-rbac"),
		},
	}
	// Cross-namespace: use labels + finalizer, not ownerRef.
	SetCrossNamespaceOwnership(cluster, &sa.ObjectMeta)
	return sa
}

// BuildClusterRole constructs the CSI ClusterRole.
// Reference: csi/deploy/rbac.yaml
func BuildClusterRole(cluster *blockv1alpha1.SeaweedBlockCluster) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   clusterRoleName,
			Labels: ClusterScopedLabels(cluster, "csi-rbac"),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"persistentvolumes"},
				Verbs:     []string{"get", "list", "watch", "create", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"persistentvolumeclaims"},
				Verbs:     []string{"get", "list", "watch", "update"},
			},
			{
				APIGroups: []string{"storage.k8s.io"},
				Resources: []string{"storageclasses"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"list", "watch", "create", "update", "patch"},
			},
			{
				APIGroups: []string{"storage.k8s.io"},
				Resources: []string{"csinodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"storage.k8s.io"},
				Resources: []string{"volumeattachments"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

// BuildClusterRoleBinding constructs the CSI ClusterRoleBinding.
// Reference: csi/deploy/rbac.yaml
func BuildClusterRoleBinding(cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:   clusterRoleBinding,
			Labels: ClusterScopedLabels(cluster, "csi-rbac"),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: csiNS,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
}
