package vr

import (
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
	"net"
	"os"
	"runtime"
	"sort"
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
		levelsToLog := []int{DEBUG, STATUS, ERROR}
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
	return
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
	mstate.HighestOp = map[uint]uint{}
	mstate.Heartbeats = map[uint]time.Time{}
}

// just closes the connections (doesn't stop timers, etc.)
func (r *Replica) ShutdownIncoming() {
	// TODO: locking
	if r.Listener != nil {
		r.Listener.Close()
	}
	for _, c := range r.Codecs {
		c.Close()
	}
	r.Codecs = []*GobServerCodec{}
}

func (r *Replica) ShutdownOutgoing() {
	r.ConnLock.Lock()
	for _, c := range r.Conns {
		if c != nil {
			c.Close()
		}
	}
	r.ConnLock.Unlock()
}

func (r *Replica) Disconnect() {
	r.IsDisconnected = true
	r.ShutdownIncoming()
	r.ShutdownOutgoing()
}

func (r *Replica) Reconnect() {
	assert(r.IsDisconnected)
	ln, err := net.Listen("tcp", r.Config[r.Rstate.ReplicaNumber])
	if err != nil {
		r.Debug(ERROR, "Couldn't start a listener: %v", err)
		return
	}
	r.Listener = ln
	r.IsDisconnected = false
	go r.ReplicaRun()
}

// gets as close as we can to a full replica shutdown. recovery is simply calling RunAsReplica again
func (r *Replica) Shutdown() {
	r.Rstate.Timer.Stop()
	r.Mstate.Timer.Stop()
	r.Disconnect()
	r.Mstate.Reset()
	r.IsShutdown = true
}

func (r *Replica) ListenerInit() error {
	ln, err := net.Listen("tcp", r.Config[r.Rstate.ReplicaNumber])
	if err != nil {
		r.Debug(ERROR, "Couldn't start a listener: %v", err)
		return err
	}
	r.Listener = ln
	return nil
}

func (r *Replica) Revive() {
	ln, err := net.Listen("tcp", r.Config[r.Rstate.ReplicaNumber])
	if err != nil {
		r.Debug(ERROR, "Couldn't start a listener: %v", err)
		return
	}
	r.Listener = ln
	r.Rstate.Timer.Reset(LEASE)
	r.Mstate.Timer.Reset(LEASE / RENEW_FACTOR)
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

type ByOpNumber []uint

func (a ByOpNumber) Len() int           { return len(a) }
func (a ByOpNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByOpNumber) Less(i, j int) bool { return a[i] < a[j] }

func SortUints(uints map[uint]uint) []uint {
	vals := make([]uint, len(uints))
	i := 0
	for _, v := range uints {
		vals[i] = v
		i++
	}
	sort.Sort(ByOpNumber(vals))
	return vals
}
