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
	Cmd  *DBCommand
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
		resp := &DBResponse{}
		switch req.Command {
		case "CHILDREN":
			kids, err := getChildren(root, req.Path)
			if resp.Error = err; err == nil {
				resp.Reply = kids
			}
		case "CREATE":
			n, err := createNode(root, req.Path, req.Value)
			if resp.Error = err; err == nil {
				resp.Reply = n
			}
		case "DELETE":
			n, err := deleteNode(root, req.Path)
			if resp.Error = err; err == nil {
				resp.Reply = n
			}
		case "EXISTS":
			n, err := existsNode(root, req.Path)
			if resp.Error = err; err == nil {
				resp.Reply = n
			}
		case "GET":
			n, err := getNode(root, req.Path)
			log.Println(err)
			if resp.Error = err; err == nil {
				resp.Reply = n
			}
		case "SET":
			n, err := setNode(root, req.Path, req.Value)
			if resp.Error = err; err == nil {
				resp.Reply = n
			}
		default:
			err := errors.New("Unknown command")
			resp.Error = err
		}
		request.Done <- resp
	}
}
