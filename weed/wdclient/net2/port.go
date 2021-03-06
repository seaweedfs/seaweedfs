package net2

import (
	"net"
	"strconv"
)

// Returns the port information.
func GetPort(addr net.Addr) (int, error) {
	_, lport, err := net.SplitHostPort(addr.String())
	if err != nil {
		return -1, err
	}
	lportInt, err := strconv.Atoi(lport)
	if err != nil {
		return -1, err
	}
	return lportInt, nil
}
