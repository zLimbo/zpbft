package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	Op []byte
}

type Stage = int32

const (
	PrepareStage = iota
	CommitStage
	ReplyStage
)

type CmdCert struct {
	seq    int64
	digest []byte
	start  time.Time
	replys map[int64][]byte
}

type LogCert struct {
	seq      int64
	view     int64
	req      *RequestArgs
	digest   []byte
	prepares map[int64][]byte
	commits  map[int64][]byte
	stage    Stage
	canReply bool
	mu       sync.Mutex
}

func (lc *LogCert) set(req *RequestArgs, digest []byte, view int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.req = req
	lc.digest = digest
}

func (lc *LogCert) get() (*RequestArgs, []byte, int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.req, lc.digest, lc.view
}

func (lc *LogCert) popAllPrepares() [][]byte {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	prepares := make([][]byte, 0, len(lc.prepares))
	for _, prepare := range lc.prepares {
		prepares = append(prepares, prepare)
	}
	return prepares
}

func (lc *LogCert) popAllCommits() [][]byte {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	commits := make([][]byte, 0, len(lc.commits))
	for _, commit := range lc.commits {
		commits = append(commits, commit)
	}
	return commits
}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}

func (lc *LogCert) prepareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepares[nodeId]
	return ok
}

func (lc *LogCert) commitVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.commits[nodeId]
	return ok
}

func (lc *LogCert) prepareVote(nodeId int64, sign []byte) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepares[nodeId] = sign
}

func (lc *LogCert) commitVote(nodeId int64, sign []byte) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commits[nodeId] = sign
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepares)
}

func (lc *LogCert) commitBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.commits)
}

func (lc *LogCert) clear() {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.req.Sign = nil
	lc.req.Req.Operator = nil
	lc.digest = nil
	lc.prepares = nil
	lc.commits = nil
}

type RequestMsg struct {
	Operator  []byte
	Timestamp int64
	ClientId  int64
}

type PrePrepareMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
}

type PrepareMsg struct {
	View          int64
	Seq           int64
	Digest        []byte
	AggregateSign []byte
	NodeId        int64
}

type CommitMsg struct {
	View          int64
	Seq           int64
	Digest        []byte
	AggregateSign []byte
	NodeId        int64
}

type ReplyMsg struct {
	View      int64
	Seq       int64
	Timestamp int64
	ClientId  int64
	NodeId    int64
	Result    []byte
}

type RequestArgs struct {
	Req  *RequestMsg
	Sign []byte
}

type RequestReply struct {
	Seq int64
	Ok  bool
}

type PrePrepareArgs struct {
	Msg     *PrePrepareMsg
	Sign    []byte
	ReqArgs *RequestArgs
}

type PrePrepareReply struct {
	Ok   bool
	Sign []byte
}

type PrepareArgs struct {
	Msg  *PrepareMsg
	Sign []byte
}

type PrepareReply struct {
	Ok   bool
	Sign []byte
}

type CommitArgs struct {
	Msg  *CommitMsg
	Sign []byte
}

type CommitReply struct {
	Ok   bool
	Sign []byte
}

type ReplyArgs struct {
	Msg  *ReplyMsg
	Sign []byte
}

type CloseCliCliArgs struct {
	ClientId int64
}
