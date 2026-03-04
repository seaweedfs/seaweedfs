#!/usr/bin/env bash
# smoke-test.sh -- deploy sw-block CSI driver to k3s/kind and verify PVC lifecycle.
# Requires: k3s or kind pre-installed, kubectl, go, docker (for kind).
# Usage: bash smoke-test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
DEPLOY_DIR="$SCRIPT_DIR/../../deploy"
BINARY="$SCRIPT_DIR/block-csi"

echo "=== sw-block CSI smoke test ==="

# 1. Build binary
echo "[1/7] Building block-csi (linux/amd64)..."
cd "$REPO_ROOT"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$BINARY" ./weed/storage/blockvol/csi/cmd/block-csi/
echo "  built: $BINARY"

# 2. Detect runtime (k3s or kind)
if command -v k3s &>/dev/null; then
    RUNTIME=k3s
    KUBECTL="k3s kubectl"
    echo "[2/7] Detected k3s"

    # Copy binary to a path accessible by the k3s node
    sudo cp "$BINARY" /usr/local/bin/block-csi
    sudo chmod +x /usr/local/bin/block-csi
elif command -v kind &>/dev/null; then
    RUNTIME=kind
    KUBECTL="kubectl"
    echo "[2/7] Detected kind"

    # Build a minimal container image
    cat > /tmp/block-csi-Dockerfile <<'DOCKERFILE'
FROM alpine:3.19
RUN apk add --no-cache open-iscsi e2fsprogs util-linux
COPY block-csi /usr/local/bin/block-csi
ENTRYPOINT ["/usr/local/bin/block-csi"]
DOCKERFILE
    docker build -t sw-block-csi:local -f /tmp/block-csi-Dockerfile "$SCRIPT_DIR"
    kind load docker-image sw-block-csi:local
else
    echo "ERROR: neither k3s nor kind found" >&2
    exit 1
fi

# 3. Deploy manifests
echo "[3/7] Deploying CSI driver..."
$KUBECTL apply -f "$DEPLOY_DIR/rbac.yaml"
$KUBECTL apply -f "$DEPLOY_DIR/csi-driver.yaml"
$KUBECTL apply -f "$DEPLOY_DIR/storageclass.yaml"

echo "  Waiting for DaemonSet to be ready..."
$KUBECTL -n kube-system rollout status daemonset/sw-block-csi-node --timeout=120s

# 4. Create PVC
echo "[4/7] Creating PVC..."
$KUBECTL apply -f "$DEPLOY_DIR/example-pvc.yaml"

echo "  Waiting for pod to be ready..."
$KUBECTL wait --for=condition=Ready pod/sw-block-test-pod --timeout=120s

# 5. Verify data
echo "[5/7] Verifying data..."
DATA=$($KUBECTL exec sw-block-test-pod -- cat /data/test.txt)
if [ "$DATA" = "hello sw-block" ]; then
    echo "  OK: data verified"
else
    echo "  FAIL: expected 'hello sw-block', got '$DATA'" >&2
    exit 1
fi

# 6. Delete and recreate pod to verify persistence
echo "[6/7] Deleting pod and verifying persistence..."
$KUBECTL delete pod sw-block-test-pod --grace-period=5
$KUBECTL apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: sw-block-test-pod
spec:
  containers:
    - name: app
      image: busybox
      command: ["sh", "-c", "cat /data/test.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: sw-block-test
EOF

$KUBECTL wait --for=condition=Ready pod/sw-block-test-pod --timeout=120s
DATA=$($KUBECTL exec sw-block-test-pod -- cat /data/test.txt)
if [ "$DATA" = "hello sw-block" ]; then
    echo "  OK: data persisted across pod restart"
else
    echo "  FAIL: data not persisted, got '$DATA'" >&2
    exit 1
fi

# 7. Cleanup
echo "[7/7] Cleaning up..."
$KUBECTL delete pod sw-block-test-pod --grace-period=5 || true
$KUBECTL delete pvc sw-block-test || true
$KUBECTL delete -f "$DEPLOY_DIR/csi-driver.yaml" || true
$KUBECTL delete -f "$DEPLOY_DIR/storageclass.yaml" || true
$KUBECTL delete -f "$DEPLOY_DIR/rbac.yaml" || true

echo "=== smoke test PASSED ==="
