package main

import (
	"flag"
	"github.com/mgentili/goPhat/phatRPC"
	"github.com/mgentili/goPhat/vr"
	"strings"
)

// starts a Paxos replica node with a given nodeset configuration and also
// starts listening for client connections
func main() {
	index := flag.Uint("index", 0, "this replica's index")
	replica_config := flag.String("replica_config", "", "list of all replica addresses separated by commas")
	rpc_config := flag.String("rpc_config", "", "list of all RPC addresses separated by commas")

	flag.Parse()

	ind := *index
	replicas := strings.Split(*replica_config, ",")
	rpcs := strings.Split(*rpc_config, ",")
	r := vr.RunAsReplica(ind, replicas)
	phatRPC.StartServer(rpcs[ind], r)

	<-make(chan int)
}
