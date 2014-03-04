package phatlog

import (
	"testing"
)

func setup() *Log {
	commitLog := &Log{}
	commitLog.Commits = make(map[int]*Command)
	return commitLog
}

func addCommits(commitLog *Log) {
	a := &Command{"create"}
	b := &Command{"delete"}
	c := &Command{"close"}
	commitLog.commit(0, a)
	commitLog.commit(1, b)
	commitLog.commit(3, c)

}

//not really a test - golang map works as expected..
//note:there is no order guarentees here, maybe use an ordered map?
func TestAdd(t *testing.T) {
	commitLog := setup()
	addCommits(commitLog)

	for key, value := range commitLog.Commits {
		t.Log("Key:", key, "Value:", value)
	}

}

//another non-test, showing off retrival of vals
//notice how it doesnt break if we have no command for an index
func TestGet(t *testing.T) {
	commitLog := setup()
	addCommits(commitLog)

	for i := 0; i < commitLog.MaxIndex+1; i++ {
		command := commitLog.getCommit(i)
		t.Log("Key:", i, "Value:", command)
	}
}
