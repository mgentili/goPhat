package phatdb

type DBCommand struct {
	Command string
	Path    string
	Value   string
}

type DBResponse struct {
	Reply interface{}
	Error string
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
		resp := &DBResponse{}
		switch req.Command {
		case "CHILDREN":
			kids, err := getChildren(root, req.Path)
			if err == nil {
				resp.Reply = kids
			} else {
				resp.Error = err.Error()
			}
		case "CREATE":
			n, err := createNode(root, req.Path, req.Value)
			if err == nil {
				resp.Reply = n
			} else {
				resp.Error = err.Error()
			}
		case "DELETE":
			n, err := deleteNode(root, req.Path)
			if err == nil {
				resp.Reply = n
			} else {
				resp.Error = err.Error()
			}
		case "EXISTS":
			n, err := existsNode(root, req.Path)
			if err == nil {
				resp.Reply = n
			} else {
				resp.Error = err.Error()
			}
		case "GET":
			n, err := getNode(root, req.Path)
			if err == nil {
				resp.Reply = n
			} else {
				resp.Error = err.Error()
			}
		case "SET":
			_, err := setNode(root, req.Path, req.Value)
			// SET doesn't return any results on success
			if err != nil {
				resp.Error = err.Error()
			}
		case "SHA256":
			resp.Reply = hashNode(root)
		default:
			resp.Error = "Unknown command"
		}
		request.Done <- resp
	}
}
