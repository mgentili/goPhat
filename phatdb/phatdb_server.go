package phatdb

import (
	"errors"
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
		case "CHILDREN":
			kids, err := getChildren(root, req.Path)
			req.Done <- &DBResponse{kids, err}
		case "CREATE":
			n, err := createNode(root, req.Path, req.Value)
			req.Done <- &DBResponse{n, err}
		case "DELETE":
			n, err := deleteNode(root, req.Path)
			req.Done <- &DBResponse{n, err}
		case "EXISTS":
			n, err := existsNode(root, req.Path)
			req.Done <- &DBResponse{n, err}
		case "GET":
			n, err := getNode(root, req.Path)
			req.Done <- &DBResponse{n, err}
		case "SET":
			n, err := setNode(root, req.Path, req.Value)
			req.Done <- &DBResponse{n, err}
		default:
			req.Done <- &DBResponse{nil, errors.New("Unknown command")}
		}
	}
}
