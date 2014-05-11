package vr

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Snapshot struct {
	SnapshotIndex uint
	Data          []byte
}

// does a snapshot (synchronous)
func (r *Replica) TakeSnapshot() {
	r.SnapshotLock.Lock()
	defer r.SnapshotLock.Unlock()
	r.Debug(STATUS, "Taking snapshot of roughly %d (current snapshot is %d)", r.Rstate.CommitNumber, r.SnapshotIndex)
	if r.Rstate.CommitNumber <= r.SnapshotIndex {
		return
	}
	bytes, snapIndex, err := r.SnapshotFunc(r.Context, func() uint { return r.Rstate.CommitNumber })
	defer func() {
		if err != nil {
			r.Debug(ERROR, err.Error())
		}
	}()
	if err != nil {
		return
	}
	// we first write to a temp file, then move it into the real location (so it happens atomically)
	tmpfile := fmt.Sprintf("%s.tmp", r.SnapshotFile)
	f, err := os.Create(tmpfile)
	if err != nil {
		return
	}
	defer f.Close()
	snapIndexBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(snapIndexBytes, uint64(snapIndex))
	_, err = f.Write(snapIndexBytes)
	if err != nil {
		return
	}
	_, err = f.Write(bytes)
	if err != nil {
		return
	}
	err = f.Sync()
	if err != nil {
		return
	}
	err = os.Rename(tmpfile, r.SnapshotFile)
	if err != nil {
		return
	}
	// TODO: compaction
	r.SnapshotIndex = snapIndex
}
