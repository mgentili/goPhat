package vr

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"
)

// Go doesn't have assertions...
func assert(b bool) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		log.Fatalf("assertion failed: %s:%d", file, line)
	}
}

func wrongView() error {
	return errors.New("view numbers don't match")
}

func (r *Replica) Debug(format string, args ...interface{}) {
	str := fmt.Sprintf("Replica %d: %s", r.Rstate.ReplicaNumber, format)
	log.Printf(str, args...)
}

func (r *Replica) IsMaster() bool {
	return r.Rstate.View%(NREPLICAS) == r.Rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
	mstate.A = 0
	mstate.Replies = 0
	mstate.Heartbeats = map[uint]time.Time{}
}

func (r *Replica) Shutdown() {
	r.Listener.Close()
	r.Rstate.Timer.Stop()
	r.Mstate.Timer.Stop()
	r.Mstate.Reset()
	r.IsShutdown = true
}

// closes connection to the given replica number
func (r *Replica) DestroyConns(repNum uint) {
	r.ConnLock.Lock()
	if r.Conns[repNum] != nil {
		r.Conns[repNum].Close()
	}
	r.ConnLock.Unlock()
}
