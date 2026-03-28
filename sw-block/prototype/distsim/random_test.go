package distsim

import "testing"

func TestRandomScenarioSeeds(t *testing.T) {
	seeds := []int64{
		1, 2, 3, 4, 5,
		11, 21, 34, 55, 89,
		101, 202, 303, 404, 505,
	}

	for _, seed := range seeds {
		seed := seed
		t.Run("seed_"+itoa64(seed), func(t *testing.T) {
			t.Parallel()
			if _, err := RunRandomScenario(seed, 60); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	buf := make([]byte, 0, 20)
	for v > 0 {
		buf = append(buf, byte('0'+v%10))
		v /= 10
	}
	if neg {
		buf = append(buf, '-')
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}
