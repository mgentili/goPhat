package main

import (
	"fmt"
)

type Replica struct {
	NumReplicas int
	Id int
	AddrList []string
}

func main() {
	fmt.Println("Starting new server");
}