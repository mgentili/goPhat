package gophat

type Server struct {
	NumReplicas int
	Id int
	AddrList []string
	isMaster bool
}

type RPCArgs struct {
	h Handle
	subpath string
	data string
}

type Command struct {
	name string
}

type Handle struct {
	sequenceNumber int64

} 

type Sequencer struct {
	lockName string
	mode bool
	lockGenerationNumber int64
}

type Metadata struct {
	instanceNumber int64
	contentGenerationNumber int64
	lockGenerationNumber int64
	ACLGenerationNumber int64
}

type LockType struct {

}