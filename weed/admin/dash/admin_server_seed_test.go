// MIGRATION: Tests for enrichConfigDefaults helpers. Remove after March 2027.
package dash

import "testing"

func TestCleanMaintenanceScript(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "only lock unlock",
			input:    "  lock\n  unlock\n",
			expected: "",
		},
		{
			name:     "strips lock and unlock",
			input:    "  lock\n  ec.balance -apply\n  volume.fix.replication -apply\n  unlock\n",
			expected: "ec.balance -apply\nvolume.fix.replication -apply",
		},
		{
			name:     "case insensitive lock",
			input:    "Lock\nec.balance -apply\nUNLOCK",
			expected: "ec.balance -apply",
		},
		{
			name:     "preserves comments removal",
			input:    "lock\n# a comment\nec.balance -apply\nunlock",
			expected: "ec.balance -apply",
		},
		{
			name:     "no lock unlock present",
			input:    "ec.balance -apply\nvolume.fix.replication -apply",
			expected: "ec.balance -apply\nvolume.fix.replication -apply",
		},
		{
			name:     "windows line endings",
			input:    "lock\r\nec.balance -apply\r\nunlock\r\n",
			expected: "ec.balance -apply",
		},
		{
			name:     "lock with inline comment",
			input:    "lock # migration\nec.balance -apply\nunlock # done",
			expected: "ec.balance -apply",
		},
		{
			name:     "command with inline comment preserved",
			input:    "lock\nec.balance -apply # rebalance shards\nunlock",
			expected: "ec.balance -apply",
		},
		{
			name:     "only inline comment after stripping",
			input:    "# full line comment\n  # indented comment\n",
			expected: "",
		},
		{
			name:     "typical master default",
			input:    "\n  lock\n  ec.encode -fullPercent=95 -quietFor=1h\n  ec.rebuild -apply\n  ec.balance -apply\n  fs.log.purge -daysAgo=7\n  volume.deleteEmpty -quietFor=24h -apply\n  volume.balance -apply\n  volume.fix.replication -apply\n  s3.clean.uploads -timeAgo=24h\n  unlock\n",
			expected: "ec.encode -fullPercent=95 -quietFor=1h\nec.rebuild -apply\nec.balance -apply\nfs.log.purge -daysAgo=7\nvolume.deleteEmpty -quietFor=24h -apply\nvolume.balance -apply\nvolume.fix.replication -apply\ns3.clean.uploads -timeAgo=24h",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanMaintenanceScript(tt.input)
			if got != tt.expected {
				t.Errorf("cleanMaintenanceScript(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
