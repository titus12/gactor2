package actor

import "google.golang.org/grpc"

type Cluster interface {
	ActorInfo(id int64) (local bool, node string, conn *grpc.ClientConn, err error)
}
