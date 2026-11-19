package command

import "github.com/seaweedfs/seaweedfs/weed/pb"

// embeddedFilerAddress returns the address a service embedded in the filer's
// process (such as S3) should use for that filer, in the form the filer
// registers in the master: ip:port.grpcPort. A zero *portGrpc gets the filer's
// default, port+10000, written back so the filer listens on the same port.
func embeddedFilerAddress(ip string, port int, portGrpc *int) string {
	if *portGrpc == 0 {
		*portGrpc = 10000 + port
	}
	return string(pb.NewServerAddress(ip, port, *portGrpc))
}
