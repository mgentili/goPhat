package gophat

import (
	"errors"
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
	InputChan       chan phatdb.DBCommand
	ResponseChan    chan *phatdb.DBResponse
}

func (s *Server) startDB() {
	log.Println("Starting DB")
	input := make(chan phatdb.DBCommand)
	response := make(chan *phatdb.DBResponse)
	s.InputChan = input
	s.ResponseChan = response
	go phatdb.DatabaseServer(input, response)
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
		reply.Error = errors.New("Not master")
		reply.Reply = s.MasterId
		return nil
	} else {
		switch args.Command {
		//if the command is a write, then we need to go through paxos
		case "CREATE", "DELETE", "SET":
			log.Println("Need to send command to paxos")
			s.InputChan <- *args
			rep := <-s.ResponseChan
			*reply = *rep
			//			paxos(args)
		//otherwise, we can go directly to the database
		default:
			log.Println("Read command")
			s.InputChan <- *args
			rep := <-s.ResponseChan
			*reply = *rep
		}
	}
	log.Println("Done with rpcdb call!")
	return nil
}
