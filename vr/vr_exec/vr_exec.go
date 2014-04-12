package main

import (
	"flag"
	"github.com/mgentili/goPhat/vr"
	"time"
	"fmt"
)

var config []string

var N int 

func RunTest(r *vr.Replica) {
	go func() {
		for {
			if r.IsShutdown || !r.IsMaster() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			//r.RunVR("foo")
			//r.RunVR("bar")
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func FailRep(shutdownRep uint, reps []*vr.Replica) {
	reps[shutdownRep].Shutdown()
	// even though we closed our listener, other replicas may still
	// have their old connection to us open, so close those too
	// (unfortunately this seems to be the best way to make this happen)
	for i := 0; i < N; i++ {
		if uint(i) == shutdownRep {
			continue
		}
		reps[i].DestroyConns(shutdownRep)
	}
}

func main() {
	oneProcP := flag.Bool("one", false, "Run VR in 1 process")
	indP := flag.Uint("r", 0, "replica num")
	flag.Parse()
	config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002",
	"127.0.0.1:9003", "127.0.0.1:9004"}
	N = len(config)
	fmt.Printf("Number of servers %d", N)
	if *oneProcP {
		reps := make([]*vr.Replica, N)
		for ind := N - 1; ind >= 0; ind-- {
			fmt.Printf("About to start %d", ind)
			reps[ind] = vr.RunAsReplica(uint(ind), config)
			fmt.Printf("Finished %d", ind)
			RunTest(reps[ind])
		}
		time.Sleep(1000 * time.Millisecond)
		FailRep(0, reps)
	} else {
		ind := *indP
		r := vr.RunAsReplica(ind, config)
		RunTest(r)
	}
	<-make(chan int)
}
