package vr

import (
	"encoding/binary"
	"fmt"
	"github.com/mgentili/goPhat/phatlog"
	"os"
)

func (r *Replica) SnapshotDiskData() (snapshot []byte) {
	f, err := os.Open(r.SnapshotFile)
	defer func() {
		if err != nil {
			r.Debug(ERROR, err.Error())
		}
	}()
	if err != nil {
		return
	}
	defer f.Close()

	fileinfo, err := f.Stat()
	if err != nil {
		return
	}
	buf := make([]byte, fileinfo.Size())
	n, err := f.Read(buf)
	if err != nil {
		return
	}
	assert(int64(n) == fileinfo.Size())

	return buf
}

func (r *Replica) LoadSnapshotFromDisk() {
	buf := r.SnapshotDiskData()
	r.LoadSnapshot(buf)
}

func (r *Replica) LoadSnapshot(data []byte) {
	snapIndex := uint(binary.LittleEndian.Uint64(data[:8]))
	// call user code
	r.LoadSnapshotFunc(r.Context, data[8:])
	r.SnapshotIndex = snapIndex
	r.Rstate.OpNumber = snapIndex
	r.Rstate.CommitNumber = snapIndex
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

// returns either just the log suffix or a snapshot and log suffix that are required to
// recover to current state from the given op number
func (r *Replica) RecoverInfoFromOpNumber(op uint) (log *phatlog.Log, snapshot []byte) {
	if !r.Phatlog.HasEntry(op) {
		// need to send snapshot too
		snapshot = r.SnapshotDiskData()
        // and whatever log we do have
        log = r.Phatlog
	} else {
        log = r.Phatlog.Suffix(op)
    }
	return
}
