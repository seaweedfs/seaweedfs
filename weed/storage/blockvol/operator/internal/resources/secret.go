package resources

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockv1alpha1 "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/operator/api/v1alpha1"
)

// CHAPSecretName returns the auto-generated CHAP secret name.
func CHAPSecretName(cluster *blockv1alpha1.SeaweedBlockCluster) string {
	return fmt.Sprintf("%s-chap", cluster.Name)
}

// BuildCHAPSecret constructs a CHAP authentication Secret with a random password.
// The secret is created once and not overwritten on subsequent reconcile loops.
// Rotation is triggered via the AnnotationRotateSecret annotation.
func BuildCHAPSecret(cluster *blockv1alpha1.SeaweedBlockCluster, csiNS string) (*corev1.Secret, error) {
	password, err := generateRandomPassword(32)
	if err != nil {
		return nil, fmt.Errorf("generate CHAP password: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CHAPSecretName(cluster),
			Namespace: csiNS,
			Labels:    ComponentLabels(cluster, "chap-secret"),
			Annotations: map[string]string{
				blockv1alpha1.AnnotationLastRotated: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"username": []byte("chap-user"),
			"password": []byte(password),
		},
	}

	// Cross-namespace: use labels + finalizer, not ownerRef.
	SetCrossNamespaceOwnership(cluster, &secret.ObjectMeta)
	return secret, nil
}

// NeedsRotation returns true if the CR's rotate-secret annotation is newer
// than the secret's last-rotated annotation.
func NeedsRotation(cluster *blockv1alpha1.SeaweedBlockCluster, existingSecret *corev1.Secret) bool {
	rotateTS, ok := cluster.Annotations[blockv1alpha1.AnnotationRotateSecret]
	if !ok || rotateTS == "" {
		return false
	}

	lastRotated, ok := existingSecret.Annotations[blockv1alpha1.AnnotationLastRotated]
	if !ok || lastRotated == "" {
		return true
	}

	requestTime, err := time.Parse(time.RFC3339, rotateTS)
	if err != nil {
		return false
	}
	lastTime, err := time.Parse(time.RFC3339, lastRotated)
	if err != nil {
		// BUG-QA-2 fix: malformed lastRotated must NOT trigger rotation every reconcile.
		// Treat unparseable timestamps as "rotation already happened" to prevent
		// infinite password churn that would break live iSCSI sessions.
		return false
	}

	return requestTime.After(lastTime)
}

// RegenerateCHAPPassword replaces the password in an existing secret and updates
// the last-rotated annotation.
func RegenerateCHAPPassword(secret *corev1.Secret) error {
	password, err := generateRandomPassword(32)
	if err != nil {
		return fmt.Errorf("generate CHAP password: %w", err)
	}

	secret.Data["password"] = []byte(password)
	if secret.Annotations == nil {
		secret.Annotations = make(map[string]string)
	}
	secret.Annotations[blockv1alpha1.AnnotationLastRotated] = time.Now().UTC().Format(time.RFC3339)
	return nil
}

func generateRandomPassword(length int) (string, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
