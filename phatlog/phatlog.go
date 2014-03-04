package phatlog

import ()

//dummy struct for testing, replace once we get an idea
//of what this will look like
type Command struct {
	name string //just a dummy val to test
}

//is map the best choice here?
type Log struct {
	Commits  map[int]*Command
	maxIndex int //highest seen index
}

//no builtin int max function??
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (l *Log) commit(index int, command *Command) {
	//should we check if this has already been commited to log?
	//in practice this would not matter, but might be useful
	//for debugging
	l.Commits[index] = command
	l.maxIndex = Max(l.maxIndex, index)

}

func (l *Log) getCommit(index int) *Command {
	return l.Commits[index]
}
