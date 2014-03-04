package gophat

import (
	"phatdb"
	"errors"
)

func startDB() {
	input := make(chan phatdb.DBCommand)
	go phatdb.DatabaseServer(input)
}

func (s *Server) GetMaster() string {

}

func (s *Server) RPCDB(args *phatdb.DBCommand, reply *phatdb.Response) error {
	if !s.isMaster {
		reply.Error = error.New("Not master")
		return nil
	} else {
		switch args.Command {
		case "CREATE", "DELETE", "SET":
			paxos(args)
		default:
			input<-args
			req.Done <- &phatdb.DBResponse{nil, errors.New("Unknown command")}
		}
	}

	return nil
}
