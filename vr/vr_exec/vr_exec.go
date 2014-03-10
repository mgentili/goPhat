package main

import (
	"flag"
	"goPhat/vr"
	"time"
)

var config = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

const N = 3

func main() {
	oneProcP := flag.Bool("1", false, "Run VR in 1 process")
	indP := flag.Uint("r", 0, "replica num")
	flag.Parse()

	if *oneProcP {
		var reps [N]*vr.Replica
		for ind := N - 1; ind >= 0; ind-- {
			reps[ind] = vr.RunAsReplica(uint(ind), config)
		}
		time.Sleep(1000 * time.Millisecond)
		shutdownRep := uint(0)
		reps[shutdownRep].Shutdown()
		// even though we closed our listener, other replicas may still
		// have their old connection to us open, so close those too
		for i := 0; i < N; i++ {
			if uint(i) == shutdownRep {
				continue
			}
			reps[i].DestroyConns(shutdownRep)
		}
	} else {
		ind := *indP
		vr.RunAsReplica(ind, config)
	}
	<-make(chan int)
}
