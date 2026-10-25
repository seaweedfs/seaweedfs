package weed_server

import "testing"

func TestStatisticsReplication(t *testing.T) {
	fs := &FilerServer{option: &FilerOption{DefaultReplication: "001"}}

	if got := fs.statisticsReplication("020"); got != "020" {
		t.Errorf("requested replication: got %q, want %q", got, "020")
	}
	if got := fs.statisticsReplication(""); got != "001" {
		t.Errorf("empty replication: got %q, want the filer default %q", got, "001")
	}

	// a filer without its own default leaves the choice to the master
	fs.option.DefaultReplication = ""
	if got := fs.statisticsReplication(""); got != "" {
		t.Errorf("empty replication without a filer default: got %q, want %q", got, "")
	}
}
