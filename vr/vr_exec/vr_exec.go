package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/mgentili/goPhat/vr"
	"time"
)

type VR_exec_command struct {
	S string
}

func (VR_exec_command) CommitFunc(context interface{}) {
	// commit is just a no-op for us
}

var config []string

var N int

func RunTest(r *vr.Replica) {
	go func() {
		for {
			if r.IsShutdown || !r.IsMaster() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			r.RunVR(VR_exec_command{"foo"})
			r.RunVR(VR_exec_command{"bar"})
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func FailRep(shutdownRep uint, reps []*vr.Replica) {
	//	reps[shutdownRep].Shutdown()
	reps[shutdownRep].Disconnect()
}

func StartRep(rep uint, reps []*vr.Replica, config []string) {
	reps[rep] = vr.RunAsReplica(rep, config)
	RunTest(reps[rep])
}

func main() {
	gob.Register(VR_exec_command{})

	oneProcP := flag.Bool("one", false, "Run VR in 1 process")
	indP := flag.Uint("r", 0, "replica num")
	flag.Parse()
	config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002",
		"127.0.0.1:9003", "127.0.0.1:9004"}
	N = len(config)
	fmt.Printf("Number of servers %d\n", N)
	if *oneProcP {
		reps := make([]*vr.Replica, N)
		for ind := N - 1; ind >= 0; ind-- {
			fmt.Printf("About to start %d", ind)
			StartRep(uint(ind), reps, config)
		}
		time.Sleep(1000 * time.Millisecond)
		fmt.Printf("Disconnecting replica 1\n")
		FailRep(1, reps)
		time.Sleep(5000 * time.Millisecond)
		fmt.Printf("reconnecting replica 1\n")
		//StartRep(1, reps, config)
		reps[1].Reconnect()
	} else {
		ind := *indP
		r := vr.RunAsReplica(ind, config)
		RunTest(r)
	}
	<-make(chan int)
}
