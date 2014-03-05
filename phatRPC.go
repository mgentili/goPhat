package gophat

import (
	"fmt"
	"github.com/mgentili/goPhat/phatdb"
	"log"
	"net"
	"net/rpc"
)

const NUM_REPLICAS = 5

type Server struct {
	Id              int
	MasterId        int
	ServerLocations []string
	InputChan       chan phatdb.DBCommandWithChannel
}

func (s *Server) startDB() {
	log.Println("Starting DB")
	input := make(chan phatdb.DBCommandWithChannel)
	s.InputChan = input
	go phatdb.DatabaseServer(input)
}

//starts a TCP server that accepts at the given port
func StartServer(port int, masterId int) *rpc.Server {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))

	listener, _ := net.ListenTCP("tcp", tcpAddr)

	newServer := rpc.NewServer()

	serve := new(Server)
	serve.Id = port % NUM_REPLICAS
	serve.MasterId = masterId % NUM_REPLICAS
	serve.startDB()

	newServer.Register(serve)

	log.Printf("Server at port %d trying to accept new connections\n", port)
	go newServer.Accept(listener)
	//log.Println("Accepted new connection?")
	return newServer
}

//returns the address of the current master replica
func (s *Server) GetMaster(args *Null, reply *int) error {
	//log.Printf("Master id %d", s.MasterId)
	*reply = s.MasterId
	return nil
}

//processes an RPC call sent by client
func (s *Server) RPCDB(args *phatdb.DBCommand, reply *phatdb.DBResponse) error {
	//if the server isn't the master, the respond with an error, and send over master's address
	log.Printf("Master id: %d, My id: %d", s.MasterId, s.Id)
	if s.Id != s.MasterId {
		log.Println("I'm not the master!")
		reply.Error = "Not master node"
		reply.Reply = s.MasterId
		return nil
	} else {
		argsWithChannel := phatdb.DBCommandWithChannel{args, make(chan *phatdb.DBResponse)}
		switch args.Command {
		//if the command is a write, then we need to go through paxos
		case "CREATE", "DELETE", "SET":
			log.Println("Need to send command via Paxos")
			s.InputChan <- argsWithChannel
			rep := <-argsWithChannel.Done
			*reply = *rep
			//paxos(args)
		default:
			log.Println("Read-only command skips Paxos")
			s.InputChan <- argsWithChannel
			rep := <-argsWithChannel.Done
			*reply = *rep
			log.Println("Finished read-only")
		}
	}
	return nil
}
