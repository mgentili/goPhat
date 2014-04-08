package main

func (t *TestMaster) testReplicaFailure() {
	t.Setup(3, 1)
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("stopnode 2")
	t.ProcessCall("createfile")
	t.ProcessCall("resumenode 2")
	t.ProcessCall("createfile")
}

func (t *TestMaster) testMasterFailure() {
	t.Setup(3, 1)
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("stopnode 0")
	t.ProcessCall("createfile")
	t.ProcessCall("resumenode 0")
	t.ProcessCall("createfile")
}

func (t *TestMaster) testTwoMasterFailure() {
	t.Setup(5, 1)
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("stopnode 0")
	t.ProcessCall("wait 1")
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("stopnode 1")
	t.ProcessCall("wait 1")
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("resumenode 0")
	t.ProcessCall("createfile")
	t.ProcessCall("wait 1")
	t.ProcessCall("resumenode 1")
	t.ProcessCall("createfile")
	t.ProcessCall("wait 2")
}

func (t *TestMaster) testLotsofReplicas() {
	t.Setup(5, 1)
	t.ProcessCall("createfile")
	t.ProcessCall("createfile")
	t.ProcessCall("createfile")
	t.ProcessCall("wait 4")
}