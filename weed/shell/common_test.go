package shell

import (
	_ "embed"
)

//go:embed volume.list.txt
var topoData string

//go:embed volume.list2.txt
var topoData2 string

//go:embed volume.ecshards.txt
var topoDataEc string

var (
	testTopology1  = parseOutput(topoData)
	testTopology2  = parseOutput(topoData2)
	testTopologyEc = parseOutput(topoDataEc)
)
