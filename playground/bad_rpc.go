package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type HiddenStruct struct {
	Says string
}

type Reply struct {
	Data interface{}
}

type Server struct{}

func (this *Server) Ping(i int64, reply *Reply) error {
	reply.Data = HiddenStruct{"pong"}
	fmt.Println("Server replied to ping with", reply.Data)
	return nil
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func server(ready chan bool) {
	rpc.Register(new(Server))
	ln, err := net.Listen("tcp", ":9999")
	handleError(err)
	ready <- true
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(c)
	}
}

func client() {
	c, err := rpc.Dial("tcp", ":9999")
	handleError(err)
	var result Reply
	fmt.Println("Sending ping...")
	err = c.Call("Server.Ping", int64(999), &result)
	fmt.Println("Received pong...")
	handleError(err)
	fmt.Println("Reply to ping: ", result)
}

func main() {
	gob.Register(Reply{})
	//gob.Register(HiddenStruct{})

	serverIsReady := make(chan bool)
	go server(serverIsReady)
	<-serverIsReady
	client()
}
