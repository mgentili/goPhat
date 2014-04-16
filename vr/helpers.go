package vr

import (
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
	"net"
	"os"
	"runtime"
	"time"
)

const (
	DEBUG = iota
	STATUS
	ERROR
)

var VR_log *level_log.Logger

func SetupVRLog() {
	if VR_log == nil {
		levelsToLog := []int{STATUS, ERROR}
		VR_log = level_log.NewLL(os.Stdout, "VR: ")
		VR_log.SetLevelsToLog(levelsToLog)
	}
}

// Go doesn't have assertions...
func assert(b bool) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		VR_log.Fatalf(ERROR, "assertion failed: %s:%d", file, line)
	}
}

func wrongView() error {
	return errors.New("view numbers don't match")
}

func (r *Replica) Debug(level int, format string, args ...interface{}) {
	str := fmt.Sprintf("r%d: %s, %s", r.Rstate.ReplicaNumber, r.replicaStateInfo(), format)
	VR_log.Printf(level, str, args...)
}

func (r *Replica) replicaStateInfo() string {
	return fmt.Sprintf("{v: %d, o: %d, c:%d}", r.Rstate.View, r.Rstate.OpNumber, r.Rstate.CommitNumber)
	//r.Debug(STATUS, "r%d: v:%d, o:%d, c:%d\n", r.Rstate.ReplicaNumber, r.Rstate.View, r.Rstate.OpNumber, r.Rstate.CommitNumber)
}

func (r *Replica) IsMaster() bool {
	// only consider ourself master if we're in Normal state!
	return r.Rstate.View%NREPLICAS == r.Rstate.ReplicaNumber && r.Rstate.Status == Normal
}

func (r *Replica) GetMasterId() uint {
	return r.Rstate.View % NREPLICAS
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

func (r *Replica) Revive() {
	ln, err := net.Listen("tcp", r.Config[r.Rstate.ReplicaNumber])
	if err != nil {
		r.Debug(ERROR, "Couldn't start a listener: %v", err)
		return
	}
	r.Listener = ln
	r.Rstate.Timer.Reset(LEASE)
	r.Mstate.Timer.Reset(LEASE/RENEW_FACTOR)
	r.IsShutdown = false
}

// closes connection to the given replica number
func (r *Replica) DestroyConns(repNum uint) {
	r.ConnLock.Lock()
	if r.Conns[repNum] != nil {
		r.Conns[repNum].Close()
	}
	r.ConnLock.Unlock()
}

func Max(a, b uint) uint {
	if a < b {
		return b
	}
	return a
}
