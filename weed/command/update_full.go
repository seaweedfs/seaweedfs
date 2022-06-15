//go:build elastic && ydb && gocdk && hdfs
// +build elastic,ydb,gocdk,hdfs

package command

//set true if gtags are set
func init() {
	isFullVersion = true
}
