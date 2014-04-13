package vr

import (
	"github.com/mgentili/goPhat/phatlog"
	"time"
)

type ViewChangeState struct {
	DoViewChangeMsgs []DoViewChangeArgs
	DoViewReplies    uint64
	StartViewReplies uint64
	StartViews       uint
	DoViews          uint
	NormalView       uint
}

type StartViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
}

type StartViewArgs struct {
	View         uint
	Log          *phatlog.Log
	OpNumber     uint
	CommitNumber uint
}

type DoViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
	Log           *phatlog.Log
	NormalView    uint
	OpNumber      uint
	CommitNumber  uint
}

func (r *Replica) resetVcstate() {
	r.Vcstate = ViewChangeState{}
	r.Vcstate.DoViewChangeMsgs = make([]DoViewChangeArgs, NREPLICAS)
}

//A replica notices that a viewchange is needed
func (r *Replica) PrepareViewChange() {
	r.resetVcstate()
	r.Rstate.Status = ViewChange
	r.Rstate.View++
	r.Debug(STATUS, "PrepareViewChange")

	args := StartViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber}

	go r.sendAndRecv(NREPLICAS-1, "RPCReplica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

//viewchange RPCs
func (t *RPCReplica) StartViewChange(args *StartViewChangeArgs, reply *int) error {
	r := t.R

	//This view is already ahead of the proposed one
	if r.Rstate.View > args.View {
		return nil
	}

	if r.Rstate.View < args.View {
		r.resetVcstate()
	}

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & r.Vcstate.StartViewReplies) != 0 {
		return nil
	}

	r.Vcstate.StartViewReplies |= 1 << args.ReplicaNumber
	r.Vcstate.StartViews++
	r.Debug(STATUS, "StartViewChange")

	//first time we have seen this viewchange message
	if r.Rstate.View < args.View {
		r.Vcstate.NormalView = r.Rstate.View //last known normal View
		r.Rstate.View = args.View
		r.Rstate.Status = ViewChange

		SVCargs := StartViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber}

		//send StartViewChange messages to all replicas
		// TODO: we may want to only do this if our own master lease times out
		// (and not necessarily if we get a StartViewChange from someone else)
		// otherwise, we can potentially ditch our master too early, violating
		// the lease contract (which implies that a new master can't be
		// elected until a majority of the old master's leases expire)
		go r.sendAndRecv(NREPLICAS-1, "RPCReplica.StartViewChange", SVCargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })
	}

	//if we have recieved enough StartViewChange messages send DoViewChange to new master
	if r.Vcstate.StartViews == F && !r.IsMaster() {
		r.Debug(STATUS, "Sending DoViewChange")
		r.Debug(STATUS, "Sending to: %d\n", r.Rstate.View%NREPLICAS)

		//DoViewChange args
		DVCargs := DoViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber,
			r.Phatlog, r.Vcstate.NormalView, r.Rstate.OpNumber, r.Rstate.CommitNumber}

		//send to new master
		r.SendOne(r.Rstate.View%NREPLICAS, "RPCReplica.DoViewChange", DVCargs, nil)
	}

	return nil
}

func (t *RPCReplica) DoViewChange(args *DoViewChangeArgs, reply *int) error {
	r := t.R

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & r.Vcstate.DoViewReplies) != 0 {
		return nil
	}

	r.Vcstate.DoViewReplies |= 1 << args.ReplicaNumber
	r.Vcstate.DoViews++
	r.Vcstate.DoViewChangeMsgs[args.ReplicaNumber] = *args
	r.Debug(STATUS, "DoViewChange")

	//We have recived enough DoViewChange messages
	if r.Vcstate.DoViews == F {
		r.Debug(STATUS, "PrepareStartView")
		//updates replica state based on replies
		r.calcMasterView()

		// TODO: we don't technically have a master lease at this point
		r.BecomeMaster()
		r.Rstate.Status = Normal
		r.Debug(STATUS, "ViewChangeComplete!")

		//send the StartView messages to all replicas
		SVargs := StartViewArgs{r.Rstate.View, r.Phatlog, r.Rstate.OpNumber, r.Rstate.CommitNumber}
		go r.sendAndRecv(NREPLICAS-1, "RPCReplica.StartView", SVargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })

	}
	return nil
}

func (t *RPCReplica) StartView(args *DoViewChangeArgs, reply *int) error {
	r := t.R
	r.Debug(STATUS, "StartView")

	r.Phatlog = args.Log
	r.Rstate.OpNumber = args.OpNumber
	r.Rstate.View = args.View //TODO: Note to self (Marco), this addition is necessary, right?
	r.Rstate.CommitNumber = args.CommitNumber
	r.Rstate.Status = Normal
	r.Rstate.ExtendLease(time.Now().Add(LEASE))

	r.resetVcstate()
	r.Debug(STATUS, "ViewChangeComplete!")

	return nil
}

// calcMasterView updates the new Master's state in accordance with its quorum of received
// DoViewChange Messages
func (r *Replica) calcMasterView() {
	r.Rstate.View = r.Vcstate.DoViewChangeMsgs[0].View

	var maxView uint = 0
	var maxCommit uint = 0
	var bestRep = DoViewChangeArgs{}

	for i := uint(0); i < NREPLICAS; i++ {
		DVCM := r.Vcstate.DoViewChangeMsgs[i]

		//choose highest view and commit number
		maxView = Max(maxView, DVCM.View)
		maxCommit = Max(maxCommit, DVCM.CommitNumber)

		//choose replica with highest (NormalView, OpNumber) pair to determine log
		if DVCM.NormalView >= bestRep.NormalView && DVCM.OpNumber > bestRep.OpNumber {
			bestRep = DVCM
		}
	}

	r.Rstate.View = maxView
	r.Rstate.CommitNumber = maxCommit
	r.Phatlog = bestRep.Log
	r.Rstate.OpNumber = bestRep.OpNumber
}
