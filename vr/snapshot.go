package vr

import (
	"encoding/binary"
	"fmt"
	"os"
)

func (r *Replica) LoadSnapshotFromDisk() {
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

	snapIndex := binary.LittleEndian.Uint64(buf[:8])

	r.LoadSnapshot(buf[8:], uint(snapIndex))
}

func (r *Replica) LoadSnapshot(data []byte, snapIndex uint) {
	r.LoadSnapshotFunc(r.Context, data)
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
