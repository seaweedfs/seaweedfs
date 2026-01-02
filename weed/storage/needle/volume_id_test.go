package needle

import "testing"

func TestNewVolumeId(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		expectErr bool
		expected  VolumeId
	}{
		{
			name:      "Valid single digit",
			input:     "1",
			expectErr: false,
			expected:  VolumeId(1),
		},
		{
			name:      "Valid multi digit",
			input:     "12345",
			expectErr: false,
			expected:  VolumeId(12345),
		},
		{
			name:      "Zero",
			input:     "0",
			expectErr: false,
			expected:  VolumeId(0),
		},
		{
			name:      "Invalid letter",
			input:     "a",
			expectErr: true,
		},
		{
			name:      "Invalid mixed",
			input:     "123abc",
			expectErr: true,
		},
		{
			name:      "Empty string",
			input:     "",
			expectErr: true,
		},
		{
			name:      "Negative number",
			input:     "-1",
			expectErr: true,
		},
		{
			name:      "Large number",
			input:     "4294967295",
			expectErr: false,
			expected:  VolumeId(4294967295),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vid, err := NewVolumeId(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Errorf("Expected error for input %q, got nil", tc.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %q: %v", tc.input, err)
				}
				if vid != tc.expected {
					t.Errorf("VolumeId mismatch: expected %d, got %d", tc.expected, vid)
				}
			}
		})
	}
}

func TestVolumeId_String(t *testing.T) {
	if str := VolumeId(10).String(); str != "10" {
		t.Errorf("to string failed")
	}

	vid := VolumeId(11)
	if str := vid.String(); str != "11" {
		t.Errorf("to string failed")
	}

	pvid := &vid
	if str := pvid.String(); str != "11" {
		t.Errorf("to string failed")
	}
}

func TestVolumeId_Next(t *testing.T) {
	testCases := []struct {
		name     string
		vid      VolumeId
		expected VolumeId
	}{
		{
			name:     "Normal increment",
			vid:      VolumeId(10),
			expected: VolumeId(11),
		},
		{
			name:     "Zero increment",
			vid:      VolumeId(0),
			expected: VolumeId(1),
		},
		{
			name:     "Large number",
			vid:      VolumeId(4294967294),
			expected: VolumeId(4294967295),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.vid.Next()
			if result != tc.expected {
				t.Errorf("Next() failed: expected %d, got %d", tc.expected, result)
			}
		})
	}

	// Test with pointer
	vid := VolumeId(11)
	pvid := &vid
	if new := pvid.Next(); new != 12 {
		t.Errorf("Next() on pointer failed")
	}
}

// BenchmarkNewVolumeId benchmarks volume ID parsing
func BenchmarkNewVolumeId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewVolumeId("12345")
	}
}

// BenchmarkVolumeId_String benchmarks string conversion
func BenchmarkVolumeId_String(b *testing.B) {
	vid := VolumeId(12345)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vid.String()
	}
}

// BenchmarkVolumeId_Next benchmarks next operation
func BenchmarkVolumeId_Next(b *testing.B) {
	vid := VolumeId(12345)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vid.Next()
	}
}
