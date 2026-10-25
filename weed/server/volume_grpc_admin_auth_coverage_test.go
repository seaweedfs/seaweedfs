package weed_server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ungatedVolumeServerRPCs are the VolumeServer gRPC methods that intentionally
// run without checkGrpcAdminAuth, each with the reason it stays open. Every
// other RPC in volume_server.proto must call the gate.
//
// The split is by caller, not by how destructive the method is: the guard
// checks the peer IP against -whiteList, and an operator's whitelist holds
// masters, shell hosts and workers -- not every peer volume server. Gating a
// call that one volume server makes to another therefore breaks replication, EC
// and tiering in exactly the way the fail-closed gate did before it was
// reverted. Those calls are listed here and need a different mechanism (a
// cluster-peer identity) before they can be closed.
var ungatedVolumeServerRPCs = map[string]string{
	// Cluster-internal: issued volume server -> volume server.
	"CopyFile":              "replica sync and EC task pull whole files from a peer",
	"ReadNeedleBlob":        "replica sync, vacuum and EC rebuild read needles from a peer",
	"ReadNeedleMeta":        "replica sync compares needle metadata across peers",
	"WriteNeedleBlob":       "replica sync repairs a peer's needle",
	"ReceiveFile":           "EC shard distribution pushes shards to a peer",
	"ReadVolumeFileStatus":  "the copy path queries the source volume server",
	"VolumeEcShardRead":     "a volume server reads EC shards held by a peer",
	"VolumeEcBlobDelete":    "EC delete is fanned out to the shard holders",
	"VolumeEcShardsInfo":    "EC verification polls shard holders",
	"VolumeEcShardsMount":   "EC shard distribution mounts on the receiving peer",
	"VolumeIncrementalCopy": "volume backup pulls increments from a peer",
	"VolumeSyncStatus":      "sync compares volume state across peers",
	"VolumeTailSender":      "the tail source streams to the receiving peer",
	"VolumeStatus":          "replica sync and the master's vacuum loop poll volume status",

	// Read-only or liveness: no state change, and gating them would break
	// health checking and monitoring without closing a write path. They do
	// disclose topology and usage detail, so gating them is a defensible
	// tightening -- it just needs to be a decision rather than an oversight,
	// which is what this list is for.
	"Ping":               "liveness probe",
	"GetState":           "read-only volume server state",
	"Query":              "read-only data query",
	"VacuumVolumeCheck":  "read-only garbage ratio; the vacuum steps that act on it are gated",
	"VolumeServerStatus": "read-only status, the gRPC counterpart of the /status page",
}

// TestVolumeServerAdminAuthCoverage fails when a VolumeServer RPC is neither
// gated by checkGrpcAdminAuth nor listed in ungatedVolumeServerRPCs with a
// reason. A new RPC therefore cannot be added without someone deciding which
// side of the boundary it sits on -- an allowlist this size drifts otherwise,
// which is how the gate ended up covering less than half the service.
func TestVolumeServerAdminAuthCoverage(t *testing.T) {
	declared := rpcNamesFromProto(t)
	if len(declared) < 40 {
		t.Fatalf("parsed only %d RPCs from the proto, expected the full service", len(declared))
	}

	gated := gatedVolumeServerMethods(t)

	for _, rpc := range declared {
		_, exempt := ungatedVolumeServerRPCs[rpc]
		switch {
		case gated[rpc] && exempt:
			t.Errorf("%s calls checkGrpcAdminAuth but is also listed as intentionally ungated; drop it from ungatedVolumeServerRPCs", rpc)
		case !gated[rpc] && !exempt:
			t.Errorf("%s does not call checkGrpcAdminAuth and is not listed in ungatedVolumeServerRPCs; "+
				"gate it, or add it with the reason it must stay open", rpc)
		}
	}

	// Keep the exemption list honest: an entry naming an RPC that no longer
	// exists hides the fact that nothing is being exempted.
	declaredSet := make(map[string]struct{}, len(declared))
	for _, rpc := range declared {
		declaredSet[rpc] = struct{}{}
	}
	var stale []string
	for rpc := range ungatedVolumeServerRPCs {
		if _, ok := declaredSet[rpc]; !ok {
			stale = append(stale, rpc)
		}
	}
	sort.Strings(stale)
	for _, rpc := range stale {
		t.Errorf("ungatedVolumeServerRPCs lists %q, which is not an RPC in volume_server.proto", rpc)
	}
}

func rpcNamesFromProto(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "pb", "volume_server.proto")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(`(?m)^\s*rpc\s+([A-Za-z0-9_]+)\s*\(`)
	var names []string
	for _, m := range re.FindAllStringSubmatch(string(data), -1) {
		names = append(names, m[1])
	}
	sort.Strings(names)
	return names
}

// gatedVolumeServerMethods reports which methods on *VolumeServer call
// checkGrpcAdminAuth anywhere in their body. It walks the AST rather than
// scanning a fixed window of lines so a guard placed after an early
// maintenance-mode check still counts.
func gatedVolumeServerMethods(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return strings.HasSuffix(fi.Name(), ".go") && !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	gated := make(map[string]bool)
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || fn.Body == nil {
					continue
				}
				if !isVolumeServerReceiver(fn.Recv) {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if ok && sel.Sel.Name == "checkGrpcAdminAuth" {
						gated[fn.Name.Name] = true
						return false
					}
					return true
				})
			}
		}
	}
	return gated
}

func isVolumeServerReceiver(recv *ast.FieldList) bool {
	if len(recv.List) != 1 {
		return false
	}
	star, ok := recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	return ok && ident.Name == "VolumeServer"
}
