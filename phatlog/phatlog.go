package phatlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"log"
	"sort"
)

//dummy struct for testing, replace once we get an idea
//of what this will look like
type Command struct {
	Name string //just a dummy val to test
}

//is map the best choice here?
type Log struct {
	Commits  map[uint]interface{}
	MaxIndex uint //highest seen index
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

func (l *Log) GetCommand(index uint) interface{} {
	return l.Commits[index]
}

// returns a sorted array of the 
func (l *Log) sort() []int {
	var keys []int
	for k := range l.Commits {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	return keys
}

func (l *Log) Map(mapfunc func(command interface{}) interface{}) ([]int, []interface{}){
	ordered_keys := l.sort()
	//log.Printf("Sorted keys %v", ordered_keys)
	answers := make([]interface{}, 0)
	for _, i := range ordered_keys {
		val := l.Commits[uint(i)]
		//log.Printf("Value for index %d is %v", i, val)
		answers = append(answers, mapfunc(val))
	}

	return ordered_keys, answers
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
