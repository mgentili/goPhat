package main

import (
	"fmt"
)

type Handle struct {
	sequenceNumber int64

} 

type Sequencer struct {
	lockName string
	mode bool
	lockGenerationNumber int64
}

type Metadata struct {
	instanceNumber int64
	contentGenerationNumber int64
	lockGenerationNumber int64
	ACLGenerationNumber int64
}

type LockType struct {

}

func getroot() Handle {
	var nh Handle
	return nh
}

func open(h Handle, subpath string) Handle{
	var nh Handle
	return nh
}

func mkfile(h Handle, subpath string, initialdata string) Handle {
	var nh Handle
	return nh
}

func mkdir(h Handle, subpath string) Handle {
	var nh Handle
	return nh
}

func getcontents(h Handle, subpath string) Handle {
	var nh Handle
	return nh
}

func putcontents(h Handle, subpath string) error {
	return nil
}

func readdir(h Handle) string {
	return "/"
}

func stat(h Handle) Metadata {
	var md Metadata
	return md
}

func flock(h Handle, lt LockType) Sequencer {
	var sq Sequencer
	return sq
}

func funlock(h Handle) Sequencer {
	var sq Sequencer
	return sq
}

func delete(h Handle) error {
	return nil
}

func main() {
	fmt.Println("Starting phat instance")
}