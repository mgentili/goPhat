package main

import (
	"fmt"
	"github.com/mgentili/goPhat/phatRPC"
	"github.com/mgentili/goPhat/phatclient"
	"github.com/mgentili/goPhat/vr"
	"log"
	"time"
)

var replica_config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}
var client_config = []string{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}

const N = 3

func main() {

	for i := 0; i < N; i++ {
		newReplica := vr.RunAsReplica(uint(i), replica_config)
		phatRPC.StartServer(client_config[i], newReplica)
	}

	cli, err := phatclient.NewClient(client_config, 0)
	if err != nil {
		log.Printf(err.Error())
	}

	log.Println("Trying to get /dev/null -- should fail")
	_, err = cli.GetData("/dev/null")
	log.Println("GOT", err.Error())

	log.Println("Creating /dev/null -- should succeed")
	_, err = cli.Create("/dev/null", "empty")
	if err != nil {
		log.Printf("Expected no error from Create, got %s", err)
	}

	fmt.Println("Trying to get /dev/null -- should succeed")
	n, err := cli.GetData("/dev/null")
	if err != nil {
		log.Printf("Expected no error from GetData, got %s", err)
	} else if "empty" != n.Value {
		log.Printf("Expected %s, got %s", "empty", n.Value)
	}

	fmt.Println("Setting /dev -- should succeed")
	err = cli.SetData("/dev", "something")
	if err != nil {
		log.Printf("Expected no error from SetData, got %s", err)
	}

	// don't exit immediately, so we can see e.g. replicas receiving commits
	time.Sleep(5 * time.Second)
}
