package raftx

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type ChangeSubtermIfRequiredCommand struct {
	rx        *raftx
	idHealthy uint64
}

func NewChangeSubtermIfRequiredCommand(rx *raftx, idHealthy uint64) *ChangeSubtermIfRequiredCommand {
	return &ChangeSubtermIfRequiredCommand{
		rx:        rx,
		idHealthy: idHealthy,
	}
}

func (c *ChangeSubtermIfRequiredCommand) Do(rn *raft.RawNode) {
	for i, voters := range c.rx.tracker.voters {
		if _, ok := voters[c.idHealthy]; ok && !c.rx.subterm.replicationSets[i].Contains(c.idHealthy) {
			c.rx.maybeStartNewSubterm(rn)
			return
		}
	}
}

type ApplyConfChangeCommand struct {
	rx *raftx
	cs *raftpb.ConfState
}

func NewApplyConfChangeCommand(rx *raftx, cs *raftpb.ConfState) *ApplyConfChangeCommand {
	return &ApplyConfChangeCommand{
		rx: rx,
		cs: cs,
	}
}
func (c *ApplyConfChangeCommand) Do(rn *raft.RawNode) {
	c.rx.applyConfChange(rn, c.cs)
}

type WitnessAckCommand struct {
	rx *raftx
}

func (c *WitnessAckCommand) Do(rn *raft.RawNode) {
	ci := c.rx.tracker.committedOnWitnessAck(c.rx.subterm.replicationSets, rn)

}
