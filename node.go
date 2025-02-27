package raftx

import (
	"context"

	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type node struct {
	rn     raft.Node
	rx     *raftx
	readyc chan raft.Ready
	stopc  chan struct{}
}

func StartNode(c *Config, peers []raft.Peer) raft.Node {
	n := &node{
		rn:     raft.StartNode(&c.Config, peers),
		rx:     &raftx{},
		readyc: make(chan raft.Ready),
		stopc:  make(chan struct{}),
	}

	go n.runReadyProxy()

	return n
}

func RestartNode(c *Config) raft.Node {
	n := &node{
		rn:     raft.RestartNode(&c.Config),
		rx:     &raftx{},
		readyc: make(chan raft.Ready),
		stopc:  make(chan struct{}),
	}

	go n.runReadyProxy()

	return n
}

func (n *node) Tick() {
	n.rn.Tick()
	n.DoCustomCommand(context.Background(), NewAfterRaftTickCommand(n.rx))
}

func (n *node) Campaign(ctx context.Context) error {
	return n.rn.Campaign(ctx)
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.rn.Propose(ctx, data)
}

func (n *node) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeI) error {
	return n.rn.ProposeConfChange(ctx, cc)
}

func (n *node) Step(ctx context.Context, msg raftpb.Message) error {
	if msg.Type == raftpb.MsgAppResp && msg.Term == 0 && msg.Reject && len(msg.Context) != 0 && string(msg.Context) == WitnessRejection {
		// When witness starts, it is deactivated and responses all messages
		// with a rejection MsgAppResp messages with 0 term and a special
		// context indicating its witness role.
		// Upon receiving such reject MsgAppResp messages, node shall mark
		// the witness and communicate with it following raftx protocol afterwards.
		// Witness will be activated after it receives witness specific messages.
		n.DoCustomCommand(context.Background(), NewActivateWitnessCommand(n.rx, msg.From))
		return nil
	}

	n.DoCustomCommand(context.Background(), NewBeforeRaftStepCommand(n.rx, &msg))
	defer func() {
		go func() {
			n.DoCustomCommand(context.Background(), NewAfterRaftStepCommand(n.rx, &msg))
		}()
	}()

	return n.rn.Step(ctx, msg)
}

func (n *node) Ready() <-chan raft.Ready {
	return n.readyc
}

func (n *node) Advance() {
	n.rn.Advance()
}

func (n *node) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	cs := n.rn.ApplyConfChange(cc)
	defer func() {
		n.DoCustomCommand(context.Background(), NewAfterRaftApplyConfChangeCommand(n.rx, cs))
	}()
	return cs
}

func (n *node) Stop() {
	n.rn.Stop()
}

func (n *node) Status() raft.Status {
	return n.rn.Status()
}

func (n *node) ReportUnreachable(id uint64) {
	n.rn.ReportUnreachable(id)
}

func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.rn.ReportSnapshot(id, status)
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	n.rn.TransferLeadership(ctx, lead, transferee)
}

func (n *node) ForgetLeader(ctx context.Context) error {
	return n.rn.ForgetLeader(ctx)
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.rn.ReadIndex(ctx, rctx)
}

func (n *node) DoCustomCommand(ctx context.Context, cmd raft.CustomCommand) error {
	return n.rn.DoCustomCommand(ctx, cmd)
}

func (n *node) runReadyProxy() {
	for {
		select {
		case r := <-n.rn.Ready():
			n.DoCustomCommand(context.Background(), NewReadyCommand(n.rx, &r, n.readyc))
		case <-n.stopc:
			return
		}
	}
}
