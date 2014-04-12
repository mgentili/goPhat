package vr

import (
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
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
	str := fmt.Sprintf("Replica %d: %s", r.Rstate.ReplicaNumber, format)
	VR_log.Printf(level, str, args...)
}

func (r *Replica) IsMaster() bool {
	return r.Rstate.View % uint(NREPLICAS) == r.Rstate.ReplicaNumber
}

func (r *Replica) GetMasterId() uint {
	return r.Rstate.View % uint(NREPLICAS)
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

func Max(a, b uint) uint {
	if a < b {
		return b
	}
	return a
}