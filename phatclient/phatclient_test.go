package phatclient

import (
	"fmt"
	"github.com/mgentili/goPhat/phatRPC"
	"github.com/mgentili/goPhat/vr"
	"log"
	"testing"
)

const BASE = 9000

var replica_config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}
var client_config = []string{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}

func TestClientConnection(t *testing.T) {
	for i := 0; i < 3; i = i + 1 {
		newReplica := vr.RunAsReplica(uint(i), replica_config)
		phatRPC.StartServer(client_config[i], newReplica)
	}

	cli, err := NewClient(client_config, 1, "1unique")
	if err != nil {
		log.Printf(err.Error())
	}

	/*h, _ := cli.GetHash()
	if h != "a78a33375e4d993cb8584a14518f4979f73136cfc597f1e47bd9a7b4790a0c82" {
		t.Errorf("Returned hash value is incorrect: %v", h)
	}*/

	log.Println("Trying to get /dev/null -- should fail")
	_, err = cli.GetData("/dev/null")
	log.Println("GOT", err.Error())
	if err == nil {
		t.Errorf("Expected error from GetData")
	}

	fmt.Println("Creating /dev/null -- should succeed")
	_, err = cli.Create("/dev/null", "empty")
	if err != nil {
		t.Errorf(fmt.Sprintf("Expected no error from Create, got %s"), err)
	}

	fmt.Println("Trying to get /dev/null -- should succeed")
	n, err := cli.GetData("/dev/null")
	if err != nil {
		t.Errorf(fmt.Sprintf("Expected no error from GetData, got %s"), err)
	} else if "empty" != n.Value {
		t.Errorf(fmt.Sprintf("Expected %s, got %s", "empty", n.Value))
	}

	fmt.Println("Setting /dev -- should succeed")
	err = cli.SetData("/dev", "something")
	if err != nil {
		t.Errorf(fmt.Sprintf("Expected no error from SetData, got %s"), err)
	}

	fmt.Println("Trying to get /dev -- should succeed")
	n, err = cli.GetData("/dev")
	if err != nil {
		t.Errorf(fmt.Sprintf("Expected no error from GetData, got %s"), err)
	} else if "something" != n.Value {
		t.Errorf(fmt.Sprintf("Expected %s, got %s", "something", n.Value))
	}
}
