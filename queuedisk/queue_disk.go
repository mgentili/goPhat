package queuedisk

import (
    "strconv"
    "bytes"
    "os"
    "encoding/gob"
    "encoding/binary"
    "io/ioutil"
    "github.com/mgentili/goPhat/phatlog"
    queue "github.com/mgentili/goPhat/phatqueue"
)

var log_file = "log.bin"
var snapshot_file = "snapshot.bin"

type MessageQueue struct {
	Queue           []queue.QMessage
	InProgress      map[string]queue.QMessage
	Id              int
    Log *phatlog.Log
	logFilename string
    logFilePtr *os.File
    snapshotFilename string
    OpsPerSnapshot int
    OpCounter int
}

func (mq *MessageQueue) Init(TmpOpsPerSnapshot int) {
	mq.InProgress = make(map[string]queue.QMessage)
	mq.logFilename = log_file
    mq.snapshotFilename = snapshot_file
	mq.Log = phatlog.EmptyLog()
    mq.OpsPerSnapshot = TmpOpsPerSnapshot
    mq.OpCounter = 0

    //recover from snapshot if available
	_, err := os.Stat(mq.snapshotFilename)
    if !os.IsNotExist(err) {
        mq.RecoverSnapshot(mq.snapshotFilename)
    }

    //recover from log if available
	_, err = os.Stat(mq.logFilename)
    if !os.IsNotExist(err) {
        mq.RecoverLog(mq.logFilename)
    }

    mq.logFilePtr, _  = os.OpenFile(mq.logFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
}

func (mq *MessageQueue) NextID() int {
	mq.Id += 1
	return mq.Id
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := queue.QMessage{strconv.Itoa(mq.NextID()), v}
	mq.Queue = append(mq.Queue, qm)
    mq.BackupLog(queue.LogEntry{Message:qm, Command:"PUSH"})
    mq.OpCounter++
    mq.CheckSnapshot()
}

func (mq *MessageQueue) Pop() *queue.QMessage {
	if mq.Len() == 0 {
		return nil
	}
    var qm queue.QMessage
    qm, mq.Queue = mq.Queue[len(mq.Queue)-1], mq.Queue[:len(mq.Queue)-1]
    mq.BackupLog(queue.LogEntry{Command:"POP"})
    mq.OpCounter++
    mq.CheckSnapshot()
	return &qm
}

//ReplayPush/Pop modify the queue in the same way, but do not add to
//logging file (since they are already there!)
func (mq *MessageQueue) ReplayPush(v interface{}) {
	qm := queue.QMessage{strconv.Itoa(mq.NextID()), v}
	mq.Queue = append(mq.Queue, qm)
}

func (mq *MessageQueue) ReplayPop() *queue.QMessage {
	if mq.Len() == 0 {
		return nil
	}
    var qm queue.QMessage
    qm, mq.Queue = mq.Queue[len(mq.Queue)-1], mq.Queue[:len(mq.Queue)-1]
	return &qm
}

func (mq *MessageQueue) Done(mId string) {
	delete(mq.InProgress, mId)
}

func (mq *MessageQueue) Len() int {
	return len(mq.Queue)
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.InProgress)
}

//periodically snapshot when we have done enough operations
func (mq *MessageQueue) CheckSnapshot() {
    if mq.OpCounter >= mq.OpsPerSnapshot {
        mq.Snapshot()
        mq.OpCounter = 0
    }
}

//recover the snapshot from disk
func (mq *MessageQueue) RecoverSnapshot(filename string) error {
    var r []byte
	r, err := ioutil.ReadFile(filename)

    dec := gob.NewDecoder(bytes.NewBuffer(r))
	err = dec.Decode(&mq.Queue)
	err = dec.Decode(&mq.InProgress)
	err = dec.Decode(&mq.Id)
	if err != nil {
		return err
    }

    return nil
}

//recover the log from disk
func (mq *MessageQueue) RecoverLog(filename string) {
	var r []byte
	r, _ = ioutil.ReadFile(filename)
    logEntries := mq.ParseLogFile(r)

    for _, entry := range logEntries {
        switch entry.Command {
            case "PUSH":
                mq.ReplayPush(entry.Message)
            case "POP":
                mq.ReplayPop()
            }
    }
}

//parses the binary log file into log entries
func (mq *MessageQueue) ParseLogFile(buffer []byte) []queue.LogEntry {
    gob.Register(queue.LogEntry{})
    logEntries := []queue.LogEntry{}

    buf_len := len(buffer)
    beg_idx := 0
    int_len := 4

    for beg_idx < buf_len {
        le := queue.LogEntry{}

        length := int(binary.LittleEndian.Uint32(buffer[beg_idx:beg_idx + int_len]))
        beg_idx += int_len
        decoder := gob.NewDecoder(bytes.NewBuffer(buffer[beg_idx:beg_idx+length]))
        decoder.Decode(&le)
        logEntries = append(logEntries, le)
        beg_idx += length
    }
    return logEntries
}

//write copy of log to disk
func (mq *MessageQueue) BackupLog(logentry queue.LogEntry) error {
    gob.Register(queue.LogEntry{})
    w := new(bytes.Buffer)
    encoder := gob.NewEncoder(w)
    err := encoder.Encode(logentry)

    if err != nil {
		return err
	}

    //write the size of the logentry to the file
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, uint32(len(w.Bytes())))
    _, err = mq.logFilePtr.Write(bs)

    //write the logentry to the file
    _, err = mq.logFilePtr.Write(w.Bytes())

	if err != nil {
		return err
	}

    mq.logFilePtr.Sync()

	return nil
}

func (mq *MessageQueue) Snapshot() {
    queuebytes, _ := mq.Bytes()
    temp_file := "tmp.bin"
    f, _  := os.OpenFile(temp_file, os.O_CREATE|os.O_WRONLY, 0666)
    f.Write(queuebytes)
    f.Sync()
    f.Close()
    os.Rename(temp_file, snapshot_file)

    //clear logfile since we just snapshotted
    mq.logFilePtr.Close()
    os.Remove(mq.logFilename)
    mq.logFilePtr, _  = os.OpenFile(mq.logFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

}

func (mq *MessageQueue) Bytes() ([]byte, error) {
	var queueState bytes.Buffer
	enc := gob.NewEncoder(&queueState)
	err := enc.Encode(mq.Queue)
	err = enc.Encode(mq.InProgress)
	err = enc.Encode(mq.Id)
	if err != nil {
		return nil, err
	}
	return queueState.Bytes(), nil
}
