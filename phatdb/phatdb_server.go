package phatdb

import (
	"errors"
	"log"
)

type DBCommand struct {
	Command string
	Path    string
	Value   string
}

type DBResponse struct {
	Reply interface{}
	Error error
}

type DBCommandWithChannel struct {
	Cmd *DBCommand
	Done chan *DBResponse
}

func DatabaseServer(input chan DBCommandWithChannel) {
	// Set up the root of the pseudo file system
	root := &FileNode{}
	root.Children = make(map[string]*FileNode)
	// Enter the command loop
	for {
		request := <-input
		req := request.Cmd
		log.Printf("Received request: %s", req.Command)
		switch req.Command {
		case "CHILDREN":
			kids, err := getChildren(root, req.Path)
			request.Done <- &DBResponse{kids, err}
		case "CREATE":
			n, err := createNode(root, req.Path, req.Value)
			request.Done <- &DBResponse{n, err}
		case "DELETE":
			n, err := deleteNode(root, req.Path)
			request.Done <- &DBResponse{n, err}
		case "EXISTS":
			n, err := existsNode(root, req.Path)
			request.Done <- &DBResponse{n, err}
		case "GET":
			n, err := getNode(root, req.Path)
			request.Done <- &DBResponse{n, err}
		case "SET":
			n, err := setNode(root, req.Path, req.Value)
			request.Done <- &DBResponse{n, err}
		default:
			request.Done <- &DBResponse{nil, errors.New("Unknown command")}
		}
	}
}
