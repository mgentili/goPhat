package main

import (
  "log"
)

// Command is what is taken by the server
// We could make it more generic by only using Interface{} instead of a type
type Command struct {
  Cmd string
  Key string
  Value string
  Response chan Node
}

// Node is what is stored in the DB
type Node struct {
  Data string
}

// Storage
type Storage struct {
  data map[string]Node
}

// RunServer takes commands from the input channel and processes them
// As it all goes through a channel, it will always be synchronous
// Think of it as a mini-Redis
func RunServer(input chan Command) {
  var s Storage
  s.data = make(map[string]Node)
  for {
    req := <-input
    log.Printf("Received request: %s", req.Cmd)
    ///
    // Calling the correct functions for a command can be made more generic
    // The values in the map should also be updated in place instead of replaced
    ///
    if req.Cmd == "SET" {
      if _, ok := s.data[req.Key]; ok {
        val := s.data[req.Key]
        val.Data = req.Value
        s.data[req.Key] = val
        if req.Response != nil {
          req.Response <- val
        }
      } else {
        s.data[req.Key] = Node{req.Value}
        if req.Response != nil {
          req.Response <- s.data[req.Key]
        }
      }
    }
    ///
    if req.Cmd == "GET" {
      if val, ok := s.data[req.Key]; ok {
        req.Response <- val
      } else {
        // Should reply with an error, but ...
        req.Response <- Node{}
      }
    }
  }
}

func main() {
  // Start the database
  dbc := make(chan Command)
  go RunServer(dbc)
  //
  dbc <- Command{"SET", "dog", "woof", nil}
  dbc <- Command{"SET", "cat", "meow", nil}
  dbc <- Command{"SET", "dog", "bowwow", nil}
  dbc <- Command{"SET", "dog", "woofwoof", nil}
  //
  reply := make(chan Node)
  dbc <- Command{"GET", "dog", "", reply}
  val := <-reply
  log.Printf("Received reply: dog = %s", val)
}
