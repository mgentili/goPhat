package phatdb

import (
	"log"
)

type DBCommand struct {
	Command string
	Path    string
	Value   string
	Done    chan *DBResponse
}

type DBResponse struct {
	Reply interface{}
	Error error
}

func DatabaseServer(input chan DBCommand) {
	// Set up the root of the pseudo file system
	root := &FileNode{}
	root.Children = make(map[string]*FileNode)
	// Enter the command loop
	for {
		req := <-input
		log.Printf("Received request: %s", req.Command)
		switch req.Command {
		case "CREATE":
			n, err := createNode(root, req.Path, req.Value)
			req.Done <- &DBResponse{n, err}
		}
	}
}
