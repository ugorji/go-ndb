// +build !cgo,!ignore !linux,!ignore

// Deprecated: Now the server is a C++ server and we communicate over the network.
// This is now effectively ignored during a build.

package ndb

import (
	"errors"
	"net/rpc"
)

var noCgoErr = errors.New("ndb client usage requires cgo on linux at this time")

type backend struct {
}

func (l *backend) Query(args *QueryArgs, qs *QueryIterResult) error {
	return noCgoErr
}
func (l *backend) svrGet(nkss []*Key, bkss [][]byte, errIfNotFound bool, res *GetResult) error {
	return noCgoErr
}
func (l *backend) svrUpdate(putkeys [][]byte, putvalues [][]byte, delkeys [][]byte) error {
	return noCgoErr
}
func (l *backend) nextLocalId(bs []byte) (nextid uint32, err error) {
	return 0, noCgoErr
}
func SetupRpcServer(rpcsvr *rpc.Server, ss *Server, qlimitmax uint16, indexes ...*Index) error {
	return noCgoErr
}
func CloseBackends() error {
	return noCgoErr
}
