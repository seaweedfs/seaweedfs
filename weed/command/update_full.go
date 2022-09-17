//go:build elastic && ydb && gocdk && tikv
// +build elastic,ydb,gocdk,tikv

package command

// set true if gtags are set
func init() {
	isFullVersion = true
}
