package phatlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"log"
)

//dummy struct for testing, replace once we get an idea
//of what this will look like
type Command struct {
	name string //just a dummy val to test
}

//is map the best choice here?
type Log struct {
	Commits  map[uint]interface{}
	MaxIndex uint // highest seen index
    MinIndex uint // lower bound of the log. Log contains entries i, MinIndex < i <= MaxIndex
}

//no builtin int max function??
func Max(a, b uint) uint {
	if a < b {
		return b
	}
	return a
}

func EmptyLog() *Log {
	l := new(Log)
	l.Commits = make(map[uint]interface{})
	return l
}

func (l *Log) Add(index uint, command interface{}) {
	//should we check if this has already been commited to log?
	//in practice this would not matter, but might be useful
	//for debugging
	l.Commits[index] = command
	l.MaxIndex = Max(l.MaxIndex, index)

}

func (l *Log) Suffix(newBegin uint) *Log {
    newLog := EmptyLog()
    newLog.MinIndex = newBegin
    for i := newBegin+1; i < l.MaxIndex; i++ {
        newLog.Add(i, l.GetCommand(i))
    }
    return newLog
}

func (l *Log) HasEntry(index uint) bool {
    return index > l.MinIndex
}

func (l *Log) GetCommand(index uint) interface{} {
	return l.Commits[index]
}

func (l *Log) HashLog() string {
	var logState bytes.Buffer
	// Encode the log state
	enc := gob.NewEncoder(&logState)
	err := enc.Encode(l)
	if err != nil {
		log.Fatal("Cannot hash the database state")
	}
	// Hash the database state
	hash := sha256.New()
	hash.Write(logState.Bytes())
	md := hash.Sum(nil)
	return hex.EncodeToString(md)
}
