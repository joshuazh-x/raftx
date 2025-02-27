package raftx

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type AfterRaftTickCommand struct {
	rx *raftx
}

func NewAfterRaftTickCommand(rx *raftx) *AfterRaftTickCommand {
	return &AfterRaftTickCommand{
		rx: rx,
	}
}

func (c *AfterRaftTickCommand) Do(rn *raft.RawNode) {
	c.rx.tick()
	if c.rx.tracker.ticks%uint64(c.rx.cfg.Config.ElectionTick) == 0 {
		c.rx.maybeStartNewSubterm(rn)
	}
}

type BeforeRaftStepCommand struct {
	rx *raftx
	m  *raftpb.Message
}

func NewBeforeRaftStepCommand(rx *raftx, m *raftpb.Message) *BeforeRaftStepCommand {
	return &BeforeRaftStepCommand{
		rx: rx,
		m:  m,
	}
}

func (c *BeforeRaftStepCommand) Do(rn *raft.RawNode) {
	st := rn.BasicStatus()
	if st.RaftState != raft.StateLeader {
		if c.m.Type == raftpb.MsgApp {
			for _, e := range c.m.Entries {
				if e.Type == EntryTypeSubterm {
					c.rx.slicer.AddSubterm(e.Term, decodeUint64(e.Data), e.Index)
				}
			}
		}
	}
}

type AfterRaftStepCommand struct {
	rx *raftx
	m  *raftpb.Message
}

func NewAfterRaftStepCommand(rx *raftx, m *raftpb.Message) *AfterRaftStepCommand {
	return &AfterRaftStepCommand{
		rx: rx,
		m:  m,
	}
}

func (c *AfterRaftStepCommand) Do(rn *raft.RawNode) {
	st := rn.BasicStatus()
	switch st.RaftState {
	case raft.StateLeader:
		if c.m.Type == raftpb.MsgAppResp {
			c.rx.maybeSendAppendToWitness(rn)
		}

		c.rx.tracker.observe(c.m.From)
		if c.rx.tracker.isReplicationSetCandidate(c.m.From) {
			c.rx.maybeStartNewSubterm(rn)
		}
	case raft.StatePreCandidate, raft.StateCandidate:

	}
}

type AfterRaftApplyConfChangeCommand struct {
	rx *raftx
	cs *raftpb.ConfState
}

func NewAfterRaftApplyConfChangeCommand(rx *raftx, cs *raftpb.ConfState) *AfterRaftApplyConfChangeCommand {
	return &AfterRaftApplyConfChangeCommand{
		rx: rx,
		cs: cs,
	}
}

func (c *AfterRaftApplyConfChangeCommand) Do(rn *raft.RawNode) {
	c.rx.applyConfChange(rn, c.cs)
}

type ReadyCommand struct {
	rx     *raftx
	ready  *raft.Ready
	readyc chan<- raft.Ready
}

func NewReadyCommand(rx *raftx, ready *raft.Ready, readyc chan<- raft.Ready) *ReadyCommand {
	return &ReadyCommand{
		rx:     rx,
		ready:  ready,
		readyc: readyc,
	}
}

func (c *ReadyCommand) Do(rn *raft.RawNode) {
	if c.ready.RaftState == raft.StateLeader && c.rx.state != raft.StateLeader {
		c.rx.becomeLeader(c.ready.Term)
	}
	c.ready.Messages = c.rx.patchMessages(c.ready.Messages)
	c.readyc <- *c.ready
}

type ActivateWitnessCommand struct {
	rx *raftx
	id uint64
}

func NewActivateWitnessCommand(rx *raftx, id uint64) *ActivateWitnessCommand {
	return &ActivateWitnessCommand{
		rx: rx,
		id: id,
	}
}

func (c *ActivateWitnessCommand) Do(rn *raft.RawNode) {
	c.rx.setWitness(c.id)
}
