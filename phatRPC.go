package gophat

func (s *Server) Getmaster(args *Null, reply *string) error {
	help := "here"
	reply = &help
	return nil
}

func (s *Server) Getroot(args *RPCArgs, reply *Handle) error {
	//cmd := Command{"getroot"}
	//cmdChan <- cmd
	//response := <-responseChan
	
	return nil
}

func (s *Server) Open(args *RPCArgs, reply *Handle) error {

	return nil
}

func (s *Server) Mkfile(args *RPCArgs, reply *Handle) error {
	
	return nil
}

func (s *Server) Mkdir(args *RPCArgs, reply *Handle) error {
	return nil
}

func (s *Server) Getcontents(args *RPCArgs, reply *Handle) error {
	return nil
}

func (s *Server) Putcontents(args *RPCArgs, reply *Handle) error {
	return nil
}

func (s *Server) Readdir(args *Handle, reply *string) error {
	return nil
}

func (s *Server) Stat(args *Handle, reply *Metadata) error {

	return nil
}

/*func (s *Server) Flock(h Handle, lt LockType, reply *Sequencer) error {
	return nil
}*/

func (s *Server) Funlock(args *Handle, reply *Sequencer) error {

	return nil
}

func (s *Server) Delete(args *Handle, reply *error) error {
	return nil
}