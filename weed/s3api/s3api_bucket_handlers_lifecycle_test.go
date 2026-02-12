package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

func TestResolveLifecycleDefaultsFromFilerConf(t *testing.T) {
	// Precedence: global (lowest), then path rules top-down (parent overrides global), then query (highest).
	// So parent path rule has priority over filer global config.

	t.Run("parent_rule_replication_takes_precedence_over_filer_config", func(t *testing.T) {
		fc := filer.NewFilerConf()
		fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
			LocationPrefix: "/buckets/",
			Replication:    "001",
		})
		repl := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.Equal(t, "001", repl, "parent path rule must override filer global config")
	})

	t.Run("falls_back_to_filer_config_when_parent_rule_replication_empty", func(t *testing.T) {
		fc := filer.NewFilerConf()
		fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
			LocationPrefix: "/buckets/",
			Replication:    "", // no replication on parent
		})
		repl := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.Equal(t, "010", repl, "replication should come from filer config when parent rule has none")
	})

	t.Run("parent_rule_empty_when_no_matching_prefix_uses_filer_config", func(t *testing.T) {
		fc := filer.NewFilerConf()
		// no rules; parent path /buckets/mybucket/ matches nothing
		repl := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.Equal(t, "010", repl, "when no path rule, use filer config replication")
	})

	t.Run("all_empty_when_no_parent_rule_and_no_filer_config", func(t *testing.T) {
		fc := filer.NewFilerConf()
		repl := resolveLifecycleDefaultsFromFilerConf(fc, "", "/buckets", "mybucket")
		assert.Empty(t, repl)
	})
}
