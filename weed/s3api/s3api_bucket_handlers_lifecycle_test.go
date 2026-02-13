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
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.NoError(t, err)
		assert.Equal(t, "001", repl, "parent path rule must override filer global config")
		assert.Equal(t, uint32(2), vgc, "volumeGrowthCount derived from replication 001 copy count (SameRackCount=1 -> 2 copies)")
	})

	t.Run("falls_back_to_filer_config_when_parent_rule_replication_empty", func(t *testing.T) {
		fc := filer.NewFilerConf()
		fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
			LocationPrefix: "/buckets/",
			Replication:    "", // no replication on parent
		})
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.NoError(t, err)
		assert.Equal(t, "010", repl, "replication should come from filer config when parent rule has none")
		assert.Equal(t, uint32(2), vgc, "volumeGrowthCount derived from replication 010 copy count")
	})

	t.Run("parent_rule_empty_when_no_matching_prefix_uses_filer_config", func(t *testing.T) {
		fc := filer.NewFilerConf()
		// no rules; parent path /buckets/mybucket/ matches nothing
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.NoError(t, err)
		assert.Equal(t, "010", repl, "when no path rule, use filer config replication")
		assert.Equal(t, uint32(2), vgc, "volumeGrowthCount derived from replication 010")
	})

	t.Run("all_empty_when_no_parent_rule_and_no_filer_config", func(t *testing.T) {
		fc := filer.NewFilerConf()
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "", "/buckets", "mybucket")
		assert.NoError(t, err)
		assert.Empty(t, repl)
		assert.Equal(t, uint32(0), vgc)
	})

	t.Run("parent_rule_volume_growth_count_used_when_set", func(t *testing.T) {
		fc := filer.NewFilerConf()
		fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
			LocationPrefix:    "/buckets/",
			Replication:       "010",
			VolumeGrowthCount: 4,
		})
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "010", "/buckets", "mybucket")
		assert.NoError(t, err)
		assert.Equal(t, "010", repl)
		assert.Equal(t, uint32(4), vgc, "parent VolumeGrowthCount must be used when set")
	})

	t.Run("invalid_replication_returns_error", func(t *testing.T) {
		fc := filer.NewFilerConf()
		fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
			LocationPrefix: "/buckets/",
			Replication:    "0x1", // invalid: non-digit
		})
		repl, vgc, err := resolveLifecycleDefaultsFromFilerConf(fc, "", "/buckets", "mybucket")
		assert.Error(t, err)
		assert.Equal(t, "0x1", repl, "replication string is still returned")
		assert.Equal(t, uint32(0), vgc, "volumeGrowthCount remains 0 when parse fails")
	})
}
