package worker

import (
	"fmt"
	"github.com/mgentili/goPhat/phatqueue"
	"github.com/mgentili/goPhat/queueRPC"
	"github.com/mgentili/goPhat/vr"
	"log"
	"testing"
	"time"
)

const BASE = 9000

var replica_config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}
var client_config = []string{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}

/*
func TestClientConnection(t *testing.T) {
	for i := 0; i < 3; i = i + 1 {
		newReplica := vr.RunAsReplica(uint(i), replica_config)
		queueRPC.StartServer(client_config[i], newReplica)
	}
	time.Sleep(time.Second)

	cli, err := NewWorker(client_config, 1, "w1")
	if err != nil {
		log.Printf(err.Error())
	}

	log.Println("Trying to push hello -- should succeed")
	err = cli.Push("hello")
	if err != nil {
		t.Errorf("Didn't expect error from Push")
	}

	fmt.Println("Trying to pop -- should succeed")
	res, err := cli.Pop()
	log.Printf("Res %v, err %v", res, err)
	if err != nil {
		t.Errorf(fmt.Sprintf("Expected no error from pop, got %s"), err)
	}
	msg := res.Reply.(phatqueue.QMessage).Value.(string)

	if "hello" != msg {
		t.Errorf(fmt.Sprintf("Expected %s, got %s", "hello", msg))
	}

	fmt.Println("Trying to pop -- should fail")
	res, err = cli.Pop()
	log.Printf("Res %v, err %v", res, err)
	if err == nil {
		t.Errorf(fmt.Sprintf("Expected error from pop, got %s"), err)
	}
}
*/

func Test10k(b *testing.T) {
	for i := 0; i < 3; i = i + 1 {
		newReplica := vr.RunAsReplica(uint(i), replica_config)
		queueRPC.StartServer(client_config[i], newReplica)
	}
	time.Sleep(time.Second)
	cli, err := NewWorker(client_config, 1, "w1")
	if err != nil {
		b.Errorf(err.Error())
	}
	//
	start := time.Now()
	for n := 0; n < 10000; n++ {
		testString := fmt.Sprintf("hello-%d", n)
		err = cli.Push(testString)
		if err != nil {
			b.Errorf(err.Error())
		}
		//
		res, err := cli.Pop()
		if err != nil {
			b.Errorf(err.Error())
		}
		msg := res.Reply.(phatqueue.QMessage).Value.(string)
		if testString != msg {
			b.Errorf("Expected %s but received %s", testString, msg)
		}
	}
	elapsed := time.Since(start)
	log.Printf("Test10k took %s", elapsed)
}
