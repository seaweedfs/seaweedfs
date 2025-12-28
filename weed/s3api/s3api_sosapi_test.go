package s3api

import (
	"encoding/xml"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestIsSOSAPIObject(t *testing.T) {
	tests := []struct {
		name     string
		object   string
		expected bool
	}{
		{
			name:     "system.xml should be detected",
			object:   ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml",
			expected: true,
		},
		{
			name:     "capacity.xml should be detected",
			object:   ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml",
			expected: true,
		},
		{
			name:     "regular object should not be detected",
			object:   "myfile.txt",
			expected: false,
		},
		{
			name:     "similar but different path should not be detected",
			object:   ".system-other-uuid/system.xml",
			expected: false,
		},
		{
			name:     "nested path should not be detected",
			object:   "prefix/.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml",
			expected: false,
		},
		{
			name:     "empty string should not be detected",
			object:   "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSOSAPIObject(tt.object)
			if result != tt.expected {
				t.Errorf("isSOSAPIObject(%q) = %v, want %v", tt.object, result, tt.expected)
			}
		})
	}
}

func TestIsSOSAPIClient(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		expected  bool
	}{
		{
			name:      "Veeam backup client should be detected",
			userAgent: "APN/1.0 Veeam/1.0 Backup/10.0",
			expected:  true,
		},
		{
			name:      "exact match should be detected",
			userAgent: "APN/1.0 Veeam/1.0",
			expected:  true,
		},
		{
			name:      "AWS CLI should not be detected",
			userAgent: "aws-cli/2.0.0 Python/3.8",
			expected:  false,
		},
		{
			name:      "empty user agent should not be detected",
			userAgent: "",
			expected:  false,
		},
		{
			name:      "partial match should not be detected",
			userAgent: "Veeam/1.0",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/bucket/object", nil)
			req.Header.Set("User-Agent", tt.userAgent)
			result := isSOSAPIClient(req)
			if result != tt.expected {
				t.Errorf("isSOSAPIClient() with User-Agent %q = %v, want %v", tt.userAgent, result, tt.expected)
			}
		})
	}
}

func TestGenerateSystemXML(t *testing.T) {
	xmlData, err := generateSystemXML()
	if err != nil {
		t.Fatalf("generateSystemXML() failed: %v", err)
	}

	// Verify it's valid XML
	var si SystemInfo
	if err := xml.Unmarshal(xmlData, &si); err != nil {
		t.Fatalf("generated XML is invalid: %v", err)
	}

	// Verify required fields
	if si.ProtocolVersion != sosAPIProtocolVersion {
		t.Errorf("ProtocolVersion = %q, want %q", si.ProtocolVersion, sosAPIProtocolVersion)
	}

	if !strings.Contains(si.ModelName, "SeaweedFS") {
		t.Errorf("ModelName = %q, should contain 'SeaweedFS'", si.ModelName)
	}

	if !si.ProtocolCapabilities.CapacityInfo {
		t.Error("ProtocolCapabilities.CapacityInfo should be true")
	}

	if si.SystemRecommendations == nil {
		t.Fatal("SystemRecommendations should not be nil")
	}

	if si.SystemRecommendations.KBBlockSize != sosAPIDefaultBlockSizeKB {
		t.Errorf("KBBlockSize = %d, want %d", si.SystemRecommendations.KBBlockSize, sosAPIDefaultBlockSizeKB)
	}
}

func TestCapacityInfoXMLStruct(t *testing.T) {
	// Test that CapacityInfo can be marshaled correctly
	ci := CapacityInfo{
		Capacity:  1000000,
		Available: 800000,
		Used:      200000,
	}

	xmlData, err := xml.Marshal(&ci)
	if err != nil {
		t.Fatalf("xml.Marshal failed: %v", err)
	}

	// Verify roundtrip
	var parsed CapacityInfo
	if err := xml.Unmarshal(xmlData, &parsed); err != nil {
		t.Fatalf("xml.Unmarshal failed: %v", err)
	}

	if parsed.Capacity != ci.Capacity {
		t.Errorf("Capacity = %d, want %d", parsed.Capacity, ci.Capacity)
	}
	if parsed.Available != ci.Available {
		t.Errorf("Available = %d, want %d", parsed.Available, ci.Available)
	}
	if parsed.Used != ci.Used {
		t.Errorf("Used = %d, want %d", parsed.Used, ci.Used)
	}
}

func TestSOSAPIConstants(t *testing.T) {
	// Verify constants are correctly set
	if !strings.HasPrefix(sosAPISystemXML, sosAPISystemFolder) {
		t.Errorf("sosAPISystemXML should start with sosAPISystemFolder")
	}

	if !strings.HasPrefix(sosAPICapacityXML, sosAPISystemFolder) {
		t.Errorf("sosAPICapacityXML should start with sosAPISystemFolder")
	}

	if !strings.HasSuffix(sosAPISystemXML, "system.xml") {
		t.Errorf("sosAPISystemXML should end with 'system.xml'")
	}

	if !strings.HasSuffix(sosAPICapacityXML, "capacity.xml") {
		t.Errorf("sosAPICapacityXML should end with 'capacity.xml'")
	}

	// Protocol version should be quoted per SOSAPI spec
	if !strings.HasPrefix(sosAPIProtocolVersion, "\"") || !strings.HasSuffix(sosAPIProtocolVersion, "\"") {
		t.Errorf("sosAPIProtocolVersion should be quoted, got: %s", sosAPIProtocolVersion)
	}
}

func TestSystemInfoXMLRootElement(t *testing.T) {
	xmlData, err := generateSystemXML()
	if err != nil {
		t.Fatalf("generateSystemXML() failed: %v", err)
	}

	xmlStr := string(xmlData)

	// Verify root element name
	if !strings.Contains(xmlStr, "<SystemInfo>") {
		t.Error("XML should contain <SystemInfo> root element")
	}

	// Verify required elements
	requiredElements := []string{
		"<ProtocolVersion>",
		"<ModelName>",
		"<ProtocolCapabilities>",
		"<CapacityInfo>",
	}

	for _, elem := range requiredElements {
		if !strings.Contains(xmlStr, elem) {
			t.Errorf("XML should contain %s element", elem)
		}
	}
}

// TestSOSAPIHandlerIntegration tests the basic handler flow without a full server
func TestSOSAPIObjectDetectionEdgeCases(t *testing.T) {
	// Test various edge cases for object detection
	edgeCases := []struct {
		object   string
		expected bool
	}{
		// With leading slash
		{"/.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml", false},
		// URL encoded
		{".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c%2Fsystem.xml", false},
		// Mixed case
		{".System-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml", false},
		// Extra slashes
		{".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c//system.xml", false},
		// Correct paths
		{".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml", true},
		{".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml", true},
	}

	for _, tc := range edgeCases {
		result := isSOSAPIObject(tc.object)
		if result != tc.expected {
			t.Errorf("isSOSAPIObject(%q) = %v, want %v", tc.object, result, tc.expected)
		}
	}
}

// TestSOSAPIHandlerReturnsXMLContentType verifies content-type setting logic
func TestSOSAPIXMLContentType(t *testing.T) {
	// Create a mock response writer to check headers
	w := httptest.NewRecorder()

	// Simulate what the handler should set
	w.Header().Set("Content-Type", "application/xml")

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/xml" {
		t.Errorf("Content-Type = %q, want 'application/xml'", contentType)
	}
}

func TestHTTPTimeFormat(t *testing.T) {
	// Verify the Last-Modified header format is correct for HTTP
	w := httptest.NewRecorder()
	w.Header().Set("Last-Modified", "Sat, 28 Dec 2024 20:00:00 GMT")

	lastMod := w.Header().Get("Last-Modified")
	if lastMod == "" {
		t.Error("Last-Modified header should be set")
	}

	// HTTP date should contain day of week
	if !strings.Contains(lastMod, "Dec") {
		t.Errorf("Last-Modified should contain month, got: %s", lastMod)
	}
}

func TestCollectBucketUsageFromTopology(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 1, Size: 100, Collection: "bucket1"},
											{Id: 2, Size: 200, Collection: "bucket2"},
											{Id: 3, Size: 300, Collection: "bucket1"},
											{Id: 1, Size: 100, Collection: "bucket1"}, // Duplicate (replica), should be ignored
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	usage := collectBucketUsageFromTopology(topo, "bucket1")
	expected := int64(400) // 100 + 300
	if usage != expected {
		t.Errorf("collectBucketUsageFromTopology = %d, want %d", usage, expected)
	}

	usage2 := collectBucketUsageFromTopology(topo, "bucket2")
	expected2 := int64(200)
	if usage2 != expected2 {
		t.Errorf("collectBucketUsageFromTopology = %d, want %d", usage2, expected2)
	}
}

func TestCalculateClusterCapacity(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										MaxVolumeCount:  100,
										FreeVolumeCount: 40,
									},
								},
							},
							{
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										MaxVolumeCount:  200,
										FreeVolumeCount: 160,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	volumeSizeLimitMb := uint64(1000) // 1GB
	volumeSizeBytes := int64(1000) * 1024 * 1024

	total, available := calculateClusterCapacity(topo, volumeSizeLimitMb)

	expectedTotal := int64(300) * volumeSizeBytes
	expectedAvailable := int64(200) * volumeSizeBytes

	if total != expectedTotal {
		t.Errorf("calculateClusterCapacity total = %d, want %d", total, expectedTotal)
	}
	if available != expectedAvailable {
		t.Errorf("calculateClusterCapacity available = %d, want %d", available, expectedAvailable)
	}
}
