package actions

import (
	"context"
	"fmt"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// TierK8s is the tier for Kubernetes/operator actions.
const TierK8s = "k8s"

// getK8sNode returns the node and resolved kubectl binary for k8s actions.
// Tries: kubectl, sudo k3s kubectl. Caches per node.
func getK8sNode(ctx context.Context, actx *tr.ActionContext, nodeName string) (*infra.Node, string, error) {
	node, err := getNode(actx, nodeName)
	if err != nil {
		return nil, "", err
	}

	cacheKey := "__kubectl_" + nodeName
	if cached := actx.Vars[cacheKey]; cached != "" {
		return node, cached, nil
	}

	// Try kubectl first.
	_, _, code, _ := node.Run(ctx, "which kubectl 2>/dev/null")
	if code == 0 {
		actx.Vars[cacheKey] = "kubectl"
		return node, "kubectl", nil
	}

	// Try k3s kubectl (needs sudo on most installs).
	_, _, code, _ = node.Run(ctx, "sudo k3s kubectl version --client 2>/dev/null")
	if code == 0 {
		actx.Vars[cacheKey] = "sudo k3s kubectl"
		return node, "sudo k3s kubectl", nil
	}

	// Fallback.
	actx.Vars[cacheKey] = "kubectl"
	return node, "kubectl", nil
}

// RegisterK8sActions registers Kubernetes/operator actions.
// These actions run kubectl commands on a node with cluster access.
func RegisterK8sActions(r *tr.Registry) {
	r.RegisterFunc("kubectl_apply", TierK8s, kubectlApply)
	r.RegisterFunc("kubectl_delete", TierK8s, kubectlDelete)
	r.RegisterFunc("kubectl_get_field", TierK8s, kubectlGetField)
	r.RegisterFunc("kubectl_wait_condition", TierK8s, kubectlWaitCondition)
	r.RegisterFunc("kubectl_set_image", TierK8s, kubectlSetImage)
	r.RegisterFunc("kubectl_assert_exists", TierK8s, kubectlAssertExists)
	r.RegisterFunc("kubectl_assert_not_exists", TierK8s, kubectlAssertNotExists)
	r.RegisterFunc("kubectl_logs", TierK8s, kubectlLogs)
	r.RegisterFunc("kubectl_rollout_status", TierK8s, kubectlRolloutStatus)
	r.RegisterFunc("kubectl_exec", TierK8s, kubectlExec)
	r.RegisterFunc("kubectl_delete_pod", TierK8s, kubectlDeletePod)
	r.RegisterFunc("kubectl_pod_ready_count", TierK8s, kubectlPodReadyCount)
	r.RegisterFunc("kubectl_label", TierK8s, kubectlLabel)
	r.RegisterFunc("kubectl_get_condition", TierK8s, kubectlGetCondition)
}

// kubectlApply applies a YAML manifest.
// Params: file (path to YAML file) OR manifest (inline YAML content), namespace (optional)
func kubectlApply(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_apply: %w", err)
	}

	var cmd string
	if file := act.Params["file"]; file != "" {
		cmd = fmt.Sprintf("%s apply -f %s", kctl, file)
	} else if manifest := act.Params["manifest"]; manifest != "" {
		cmd = fmt.Sprintf("cat <<'SWEOF' | %s apply -f -\n%s\nSWEOF", kctl, manifest)
	} else {
		return nil, fmt.Errorf("kubectl_apply: file or manifest param required")
	}

	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_apply: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlDelete deletes a Kubernetes resource.
// Params: resource (e.g. "deployment/foo"), namespace (optional), wait (optional, "true" to wait)
func kubectlDelete(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_delete: resource param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_delete: %w", err)
	}

	cmd := fmt.Sprintf("%s delete %s", kctl, resource)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}
	if act.Params["wait"] == "true" {
		cmd += " --wait=true"
	}
	cmd += " --ignore-not-found"

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_delete: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlGetField gets a jsonpath field from a resource.
// Params: resource, jsonpath, namespace (optional)
func kubectlGetField(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_get_field: resource param required")
	}
	jsonpath := act.Params["jsonpath"]
	if jsonpath == "" {
		return nil, fmt.Errorf("kubectl_get_field: jsonpath param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_get_field: %w", err)
	}

	cmd := fmt.Sprintf("%s get %s -o jsonpath='%s'", kctl, resource, jsonpath)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_get_field: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlWaitCondition waits for a condition on a resource.
// Params: resource, condition (e.g. "CSIReady=True"), namespace (optional),
//
//	timeout (e.g. "5m", default "2m")
//
// Uses jsonpath polling since K8s custom conditions aren't supported by `kubectl wait`.
func kubectlWaitCondition(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_wait_condition: resource param required")
	}
	condition := act.Params["condition"]
	if condition == "" {
		return nil, fmt.Errorf("kubectl_wait_condition: condition param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_wait_condition: %w", err)
	}

	parts := strings.SplitN(condition, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("kubectl_wait_condition: condition must be Type=Status (got %q)", condition)
	}
	condType := parts[0]
	condExpected := parts[1]

	timeout := 2 * time.Minute
	if t := act.Params["timeout"]; t != "" {
		if d, parseErr := time.ParseDuration(t); parseErr == nil {
			timeout = d
		}
	}

	jsonpath := fmt.Sprintf("{.status.conditions[?(@.type=='%s')].status}", condType)
	nsFlag := ""
	if ns := act.Params["namespace"]; ns != "" {
		nsFlag = fmt.Sprintf(" -n %s", ns)
	}

	cmd := fmt.Sprintf("%s get %s%s -o jsonpath='%s'", kctl, resource, nsFlag, jsonpath)

	deadline := time.Now().Add(timeout)
	for {
		stdout, _, code, _ := node.Run(ctx, cmd)
		value := strings.TrimSpace(stdout)
		if code == 0 && value == condExpected {
			actx.Log("  condition %s=%s met", condType, condExpected)
			return map[string]string{"value": value}, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("kubectl_wait_condition: timeout waiting for %s=%s on %s (last value: %q)",
				condType, condExpected, resource, value)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// kubectlSetImage sets a container image on a deployment/statefulset.
// Params: deployment, container, image, namespace (optional)
func kubectlSetImage(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	deployment := act.Params["deployment"]
	if deployment == "" {
		return nil, fmt.Errorf("kubectl_set_image: deployment param required")
	}
	container := act.Params["container"]
	if container == "" {
		return nil, fmt.Errorf("kubectl_set_image: container param required")
	}
	image := act.Params["image"]
	if image == "" {
		return nil, fmt.Errorf("kubectl_set_image: image param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_set_image: %w", err)
	}

	cmd := fmt.Sprintf("%s set image %s %s=%s", kctl, deployment, container, image)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_set_image: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlAssertExists asserts a resource exists.
// Params: resource, namespace (optional)
func kubectlAssertExists(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_assert_exists: resource param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_assert_exists: %w", err)
	}

	cmd := fmt.Sprintf("%s get %s -o name", kctl, resource)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_assert_exists: %s not found (code=%d stderr=%s)", resource, code, stderr)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlAssertNotExists asserts a resource does NOT exist.
// Params: resource, namespace (optional)
func kubectlAssertNotExists(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_assert_not_exists: resource param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_assert_not_exists: %w", err)
	}

	cmd := fmt.Sprintf("%s get %s -o name 2>/dev/null", kctl, resource)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, _, code, _ := node.Run(ctx, cmd)
	if code == 0 && strings.TrimSpace(stdout) != "" {
		return nil, fmt.Errorf("kubectl_assert_not_exists: %s still exists", resource)
	}

	return nil, nil
}

// kubectlLogs collects logs from a pod or deployment.
// Params: resource, namespace (optional), tail (default "100"), container (optional)
func kubectlLogs(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_logs: resource param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_logs: %w", err)
	}

	tail := act.Params["tail"]
	if tail == "" {
		tail = "100"
	}

	cmd := fmt.Sprintf("%s logs %s --tail=%s", kctl, resource, tail)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}
	if container := act.Params["container"]; container != "" {
		cmd += fmt.Sprintf(" -c %s", container)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_logs: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlRolloutStatus waits for a rollout to complete.
// Params: resource, namespace (optional), timeout (default "5m")
func kubectlRolloutStatus(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_rollout_status: resource param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_rollout_status: %w", err)
	}

	timeout := act.Params["timeout"]
	if timeout == "" {
		timeout = "5m"
	}

	cmd := fmt.Sprintf("%s rollout status %s --timeout=%s", kctl, resource, timeout)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_rollout_status: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlExec runs a command inside a pod.
// Params: pod, cmd, namespace (optional), container (optional)
func kubectlExec(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	pod := act.Params["pod"]
	if pod == "" {
		return nil, fmt.Errorf("kubectl_exec: pod param required")
	}
	execCmd := act.Params["cmd"]
	if execCmd == "" {
		return nil, fmt.Errorf("kubectl_exec: cmd param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_exec: %w", err)
	}

	cmd := fmt.Sprintf("%s exec %s", kctl, pod)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}
	if container := act.Params["container"]; container != "" {
		cmd += fmt.Sprintf(" -c %s", container)
	}
	cmd += fmt.Sprintf(" -- %s", execCmd)

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_exec: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlDeletePod deletes a pod by label selector (simulates crash/kill).
// Params: selector, namespace (optional), grace_period (default "0")
func kubectlDeletePod(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	selector := act.Params["selector"]
	if selector == "" {
		return nil, fmt.Errorf("kubectl_delete_pod: selector param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_delete_pod: %w", err)
	}

	grace := act.Params["grace_period"]
	if grace == "" {
		grace = "0"
	}

	cmd := fmt.Sprintf("%s delete pod -l %s --grace-period=%s --force", kctl, selector, grace)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_delete_pod: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlPodReadyCount counts ready pods matching a label selector.
// Params: selector, namespace (optional)
// Returns: value = count of ready pods
func kubectlPodReadyCount(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	selector := act.Params["selector"]
	if selector == "" {
		return nil, fmt.Errorf("kubectl_pod_ready_count: selector param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_pod_ready_count: %w", err)
	}

	cmd := fmt.Sprintf("%s get pods -l %s -o jsonpath='{range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}'",
		kctl, selector)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}

	stdout, _, code, _ := node.Run(ctx, cmd)
	if code != 0 {
		return map[string]string{"value": "0"}, nil
	}

	count := 0
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "True" {
			count++
		}
	}

	return map[string]string{"value": fmt.Sprintf("%d", count)}, nil
}

// kubectlLabel sets or removes labels on a resource.
// Params: resource, labels, namespace (optional), overwrite ("true" to allow)
func kubectlLabel(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_label: resource param required")
	}
	labels := act.Params["labels"]
	if labels == "" {
		return nil, fmt.Errorf("kubectl_label: labels param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_label: %w", err)
	}

	cmd := fmt.Sprintf("%s label %s %s", kctl, resource, labels)
	if ns := act.Params["namespace"]; ns != "" {
		cmd += fmt.Sprintf(" -n %s", ns)
	}
	if act.Params["overwrite"] == "true" {
		cmd += " --overwrite"
	}

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kubectl_label: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kubectlGetCondition gets a specific condition's status from a CRD resource.
// Params: resource, condition_type, namespace (optional)
// Returns: value = condition status, message = condition message
func kubectlGetCondition(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	resource := act.Params["resource"]
	if resource == "" {
		return nil, fmt.Errorf("kubectl_get_condition: resource param required")
	}
	condType := act.Params["condition_type"]
	if condType == "" {
		return nil, fmt.Errorf("kubectl_get_condition: condition_type param required")
	}

	node, kctl, err := getK8sNode(ctx, actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kubectl_get_condition: %w", err)
	}

	nsFlag := ""
	if ns := act.Params["namespace"]; ns != "" {
		nsFlag = fmt.Sprintf(" -n %s", ns)
	}

	statusCmd := fmt.Sprintf("%s get %s%s -o jsonpath='{.status.conditions[?(@.type==\"%s\")].status}'",
		kctl, resource, nsFlag, condType)
	statusOut, _, _, _ := node.Run(ctx, statusCmd)

	msgCmd := fmt.Sprintf("%s get %s%s -o jsonpath='{.status.conditions[?(@.type==\"%s\")].message}'",
		kctl, resource, nsFlag, condType)
	msgOut, _, _, _ := node.Run(ctx, msgCmd)

	return map[string]string{
		"value":   strings.TrimSpace(statusOut),
		"message": strings.TrimSpace(msgOut),
	}, nil
}
