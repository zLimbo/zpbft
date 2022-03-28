package main

import (
	"log"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	node      *Node
	seqCh     chan int64
	logs      []*Log
	seq2cert  map[int64]*LogCert
	id2srvCli map[int64]*rpc.Client
	id2cliCli map[int64]*rpc.Client
	mu        sync.Mutex
	seqInc    int64
	view      int64
}

func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.seqInc, 1e10)
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:      seq,
			prepares: make(map[int64][]byte),
			commits:  make(map[int64][]byte),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) cleanCert(seq int64) {
	cert := s.getCertOrNew(seq)
	cert.clear()
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {
	// 放入请求队列直接返回，后续异步通知客户端

	Debug("RequestRpc, from: %d", args.Req.ClientId)

	// server构造 req 发送
	req := &RequestMsg{
		Operator:  make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		ClientId:  args.Req.ClientId,
	}
	node := GetNode(args.Req.ClientId)
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, node.priKey)

	args = &RequestArgs{
		Req:  req,
		Sign: sign,
	}

	// leader 分配seq
	seq := s.assignSeq()
	s.getCertOrNew(seq).set(args, digest, s.view)
	s.seqCh <- seq

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	return nil
}

func (s *Server) PrePrepare(seq int64) {
	Debug("PrePrepare, seq: %d", seq)
	cert := s.getCertOrNew(seq)
	req, digest, view := cert.get()
	msg := &PrePrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数
	args := &PrePrepareArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}

	// 发送 PrePrepare 消息，接受 2f 个签名后进入下一阶段

	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		// 异步发送
		go func() {
			var reply PrePrepareReply
			err := srvCli1.Call("Server.PrePrepareRpc", args, &reply)
			if err != nil {
				Error("Server.PrePrepareRpc %d error: %v", id1, err)
			}
			// 验证签名
			if reply.Ok && !cert.prepareVoted(id1) &&
				RsaVerifyWithSha256(digest, reply.Sign, GetNode(id1).pubKey) {
				cert.prepareVote(id1, reply.Sign)
				// 进入 prepare 阶段
				if cert.prepareBallot() >= 2*KConfig.FalultNum {
					s.Prepare(seq)
				}
			}
		}()
	}
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *PrePrepareReply) error {
	msg := args.Msg
	Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 预设返回失败
	reply.Ok = false

	// 验证PrePrepareMsg
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrePrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// 验证RequestMsg
	reqArgs := args.ReqArgs
	node = GetNode(reqArgs.Req.ClientId)
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		Warn("PrePrepareMsg error, req.digest != msg.Digest")
		return nil
	}
	ok = RsaVerifyWithSha256(digest, reqArgs.Sign, node.pubKey)
	if !ok {
		Warn("RequestMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// 设置证明
	cert := s.getCertOrNew(msg.Seq)
	cert.set(reqArgs, digest, msg.View)

	// 返回成对req摘要的签名
	reply.Sign = RsaSignWithSha256(digest, s.node.priKey)
	reply.Ok = true

	return nil
}

func (s *Server) Prepare(seq int64) {
	Debug("Prepare, seq: %d", seq)
	// TODO: 聚合签名，此处只是暂时将其合并
	cert := s.getCertOrNew(seq)
	aggregateSign := make([]byte, 0)
	for _, sign := range cert.popAllPrepares() {
		aggregateSign = append(aggregateSign, sign...)
	}

	_, digest, view := cert.get()
	msg := &PrepareMsg{
		View:          view,
		Seq:           seq,
		Digest:        digest,
		AggregateSign: aggregateSign,
		NodeId:        s.node.id,
	}

	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &PrepareArgs{
		Msg:  msg,
		Sign: sign,
	}

	// 对聚合签名摘要，也是commit消息验签的对象，所以这里先计算出
	digest = Sha256Digest(aggregateSign)

	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply PrepareReply
			err := srvCli1.Call("Server.PrepareRpc", args, &reply)
			if err != nil {
				Error("Server.PrepareRpc %d error: %v", id1, err)
			}
			// 验证签名
			if reply.Ok && !cert.commitVoted(id1) &&
				RsaVerifyWithSha256(digest, reply.Sign, GetNode(id1).pubKey) {
				cert.commitVote(id1, reply.Sign)
				// 进入 commit 阶段
				if cert.commitBallot() >= 2*KConfig.FalultNum {
					s.Commit(seq)
				}
			}
		}()
	}
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *PrepareReply) error {
	msg := args.Msg
	Debug("PrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 预设返回失败
	reply.Ok = false

	// 验证PrepareMsg
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// TODO：验证聚合签名，暂无
	aggregateSign := args.Msg.AggregateSign

	// 对聚合签名做签名
	digest = Sha256Digest(aggregateSign)
	sign := RsaSignWithSha256(digest, s.node.priKey)

	reply.Sign = sign
	reply.Ok = true

	return nil
}

func (s *Server) Commit(seq int64) {
	Debug("Commit, seq: %d", seq)
	// TODO: 聚合签名，此处只是暂时将其合并
	cert := s.getCertOrNew(seq)
	aggregateSign := make([]byte, 0)
	for _, sign := range cert.popAllCommits() {
		aggregateSign = append(aggregateSign, sign...)
	}

	_, digest, view := cert.get()
	msg := &CommitMsg{
		View:          view,
		Seq:           seq,
		Digest:        digest,
		AggregateSign: aggregateSign,
		NodeId:        s.node.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply CommitReply
			err := srvCli1.Call("Server.CommitRpc", args, &reply)
			if err != nil {
				Error("Server.CommitRpc %d error: %v", id1, err)
			}
		}()
	}
}

func (s *Server) CommitRpc(args *CommitArgs, reply *CommitReply) error {
	msg := args.Msg
	Debug("CommitRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 预设返回失败
	reply.Ok = false

	// 验证CommitMsg
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("CommitRpc verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// TODO：验证聚合签名，暂无
	// aggregateSign := args.Msg.AggregateSign

	go s.Reply(args.Msg.Seq)

	reply.Ok = true
	return nil
}

func (s *Server) Reply(seq int64) {
	Debug("Reply %d", seq)
	_, _, view := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:      view,
		Seq:       seq,
		Timestamp: time.Now().UnixNano(),
		// ClientId:  req.Req.ClientId,
		NodeId: s.node.id,
		// Result:    req.Req.Operator,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	var reply bool
	clientId := KConfig.ClientNode.id
	cliCli := s.getCliCli(clientId)
	if cliCli == nil {
		Warn("can't connect client %d", clientId)
		return
	}
	err := cliCli.Call("Client.ReplyRpc", replyArgs, &reply)
	if err != nil {
		Warn("Client.ReplyRpc error: %v", err)
		s.closeCliCli(clientId)
	}
	s.cleanCert(seq)
}

func (s *Server) getCliCli(clientId int64) *rpc.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	cliCli, ok := s.id2cliCli[clientId]
	if !ok || cliCli == nil {
		node := GetNode(clientId)
		var err error
		cliCli, err = rpc.DialHTTP("tcp", node.addr)
		if err != nil {
			Warn("connect client %d error: %v", node.addr, err)
			return nil
		}
		s.id2cliCli[clientId] = cliCli
	}
	return cliCli
}

func (s *Server) closeCliCli(clientId int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	Info("close connect with client %d", clientId)
	cliCli, ok := s.id2cliCli[clientId]
	if ok && cliCli != nil {
		cliCli = nil
		delete(s.id2cliCli, clientId)
	}
}

func (s *Server) CloseCliCliRPC(args *CloseCliCliArgs, reply *bool) error {
	s.closeCliCli(args.ClientId)
	*reply = true
	return nil
}

func (s *Server) connect() {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		Info("build connect...")
		ok = true
		for id, node := range KConfig.Id2Node {
			if node == s.node {
				continue
			}
			if s.id2srvCli[id] == nil {
				cli, err := rpc.DialHTTP("tcp", node.addr)
				if err != nil {
					Warn("connect %s error: %v", node.addr, err)
					ok = false
				} else {
					s.id2srvCli[id] = cli
				}

			}
		}
	}
	Info("== connect success ==")
}

func (s *Server) workLoop() {
	Info("== start work loop ==")

	for seq := range s.seqCh {
		s.PrePrepare(seq)
	}
}

func (s *Server) Start() {
	s.connect()
	s.workLoop()
}

func RunServer(id int64) {
	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		seq2cert:  make(map[int64]*LogCert),
		id2srvCli: make(map[int64]*rpc.Client),
		id2cliCli: make(map[int64]*rpc.Client),
	}
	// 每个分配序号后缀为节点id(8位)
	server.seqInc = server.node.id
	// 当前暂无view-change, view暂且设置为server id
	server.view = server.node.id

	go server.Start()

	rpc.Register(server)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(server.node.addr, nil); err != nil {
		log.Fatal("server error: ", err)
	}
}
