package phatlog

import (
	"testing"
)

/*func setup() *Log {
	commitLog := &Log{}
	commitLog.Commits = make(map[int]*Command)
	return commitLog
}*/

func addCommits(commitLog *Log) {
	a := &Command{"create"}
	b := &Command{"delete"}
	c := &Command{"close"}
	commitLog.Add(0, a)
	commitLog.Add(1, b)
	commitLog.Add(3, c)

}

//not really a test - golang map works as expected..
//note:there is no order guarentees here, maybe use an ordered map?
func TestAdd(t *testing.T) {
	commitLog := EmptyLog()
	addCommits(commitLog)

	for key, value := range commitLog.Commits {
		t.Log("Key:", key, "Value:", value)
	}

}

//another non-test, showing off retrieval of vals
//notice how it doesnt break if we have no command for an index
func TestGet(t *testing.T) {
	commitLog := EmptyLog()
	addCommits(commitLog)

	for i := uint(0); i < commitLog.MaxIndex+1; i++ {
		command := commitLog.GetCommand(i)
		t.Log("Key:", i, "Value:", command)
	}
}

func TestMap(t *testing.T) {
	commitLog := EmptyLog()
	addCommits(commitLog)
	mapfunc := func(c interface{}) interface{} {
		return c.(*Command).Name
	}
	keys, responses := commitLog.Map(mapfunc)
	for i, _ := range responses {
		t.Log("Key: %v, Response: %v", keys[i], responses[i])
	}
}
