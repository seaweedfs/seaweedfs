//go:build elastic && gocdk && rclone && sqlite && tikv && ydb
// +build elastic,gocdk,rclone,sqlite,tikv,ydb

package command

// set true if gtags are set
func init() {
	isFullVersion = true
}
