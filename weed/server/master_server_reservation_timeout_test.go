package weed_server

import (
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestParseReservationTimeout(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(v *util.ViperProxy)
		expected time.Duration
	}{
		{
			name:     "unset defaults to 5 minutes",
			setup:    func(v *util.ViperProxy) {},
			expected: 5 * time.Minute,
		},
		{
			name: "duration string 10m",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", "10m")
			},
			expected: 10 * time.Minute,
		},
		{
			name: "duration string 30s",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", "30s")
			},
			expected: 30 * time.Second,
		},
		{
			name: "integer seconds 300",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", 300)
			},
			expected: 300 * time.Second,
		},
		{
			name: "large integer stays seconds not nanoseconds",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", 1000000000)
			},
			expected: 1000000000 * time.Second,
		},
		{
			name: "non-positive zero defaults to 5 minutes",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", 0)
			},
			expected: 5 * time.Minute,
		},
		{
			name: "negative seconds defaults to 5 minutes",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", -10)
			},
			expected: 5 * time.Minute,
		},
		{
			name: "invalid string defaults to 5 minutes",
			setup: func(v *util.ViperProxy) {
				v.Set("master.volume_growth.reservation_timeout", "invalid")
			},
			expected: 5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawViper := viper.New()
			vp := util.NewViperProxy(rawViper)
			tt.setup(vp)
			actual := parseReservationTimeout(vp)
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}
