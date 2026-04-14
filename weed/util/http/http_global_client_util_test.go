package http

import "testing"

func TestAppendQueryParameter(t *testing.T) {
	t.Run("without existing query", func(t *testing.T) {
		actual := AppendQueryParameter("http://example.com/3,abc", "readDeleted", "true")
		expected := "http://example.com/3,abc?readDeleted=true"
		if actual != expected {
			t.Fatalf("expected %q, got %q", expected, actual)
		}
	})

	t.Run("with existing query", func(t *testing.T) {
		actual := AppendQueryParameter("http://example.com/?proxyChunkId=3,abc", "readDeleted", "true")
		expected := "http://example.com/?proxyChunkId=3,abc&readDeleted=true"
		if actual != expected {
			t.Fatalf("expected %q, got %q", expected, actual)
		}
	})
}
