package main

import (
	"net/rpc"
	"sync/atomic"
	"time"
	"zpbft/zlog"
)

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {
	// 放入请求队列直接返回，后续异步通知客户端

	zlog.Info("req from: %d", args.Req.ClientAddr)

	// 客户端的请求暂不验证
	// leader 分配seq
	seq := s.assignSeq()
	digest := Sha256Digest(args.Req)
	s.getCertOrNew(seq).set(args, digest, s.view)

	go func() {
		// 通道可能会阻塞
		s.seqCh <- seq
	}()

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	return nil
}

func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.maxSeq, 1)
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:        seq,
			id2prepare: make(map[int32]*PrepareArgs),
			id2commit:  make(map[int32]*CommitArgs),
			prepareWQ:  make([]*PrepareArgs, 0),
			commitWQ:   make([]*CommitArgs, 0),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) PrePrepare(seq int64) {
	req, digest, view := s.getCertOrNew(seq).get()
	msg := &PrePrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.prikey)
	// 配置rpc参数
	args := &PrePrepareArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}

	// pre-prepare广播
	count := int32(0)
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		go func() {
			var ok bool
			err := p.rpcCli.Call("Server.PrePrepareRpc", args, &ok)
			if err != nil {
				zlog.Error("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
			// 等待发完2f个节点再进入下一阶段
			if ok && atomic.AddInt32(&count, 1) == 2*int32(s.f) {
				zlog.Info("PrePrepare %d ok", seq)
				s.Prepare(seq)
			}
		}()
	}
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *bool) error {
	msg := args.Msg
	zlog.Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)
	// 预设返回失败
	*reply = false

	// 验证PrePrepareMsg
	peer := s.peers[args.Msg.PeerId]
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, peer.pubkey)
	if !ok {
		zlog.Warn("PrePrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
		return nil
	}

	// 验证RequestMsg
	reqArgs := args.ReqArgs
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		zlog.Warn("PrePrepareMsg error, req.digest != msg.Digest")
		return nil
	}
	// ok = RsaVerifyWithSha256(digest, reqArgs.Sign, peer.pubkey)
	// if !ok {
	// 	zlog.Warn("RequestMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
	// 	return nil
	// }

	// 设置证明
	cert := s.getCertOrNew(msg.Seq)
	cert.set(reqArgs, digest, msg.View)

	// 进入Prepare投票
	go s.Prepare(cert.seq)

	// 尝试计票
	s.verifyBallot(cert)

	// 返回成功
	*reply = true
	return nil
}

func (s *Server) Prepare(seq int64) {
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.prikey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &PrepareArgs{
		Msg:  msg,
		Sign: sign,
	}
	// prepare广播
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		go func() {
			var ok bool
			err := p.rpcCli.Call("Server.PrepareRpc", args, &ok)
			if err != nil {
				zlog.Error("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
		}()
	}
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *bool) error {
	msg := args.Msg
	zlog.Debug("PrepareRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushPrepare(args)

	// 尝试计票
	s.verifyBallot(cert)

	*reply = true
	return nil
}

func (s *Server) Commit(seq int64) {
	// cmd := s.getCertOrNew(seq).getCmd() // req一定存在
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &CommitMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.prikey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: sign,
	}
	// commit广播
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		go func() {
			var ok bool
			err := p.rpcCli.Call("Server.CommitRpc", args, &ok)
			if err != nil {
				zlog.Error("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
		}()
	}
}

func (s *Server) CommitRpc(args *CommitArgs, reply *bool) error {
	msg := args.Msg
	zlog.Debug("CommitRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushCommit(args)

	// 尝试计票
	s.verifyBallot(cert)

	*reply = true
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	req, reqDigest, view := cert.get()

	// cmd 为空则不进行后续阶段
	if req == nil {
		zlog.Debug("march, cmd is nil")
		return
	}
	// zlog.Debug("march, seq: %d, prepare: %d, commit: %d, stage: %d",
	// 	cert.seq, cert.prepareBallot(), cert.commitBallot(), cert.getStage())

	switch cert.getStage() {
	case PrepareStage:
		argsQ := cert.popAllPrepares()
		for _, args := range argsQ {
			msg := args.Msg
			if cert.prepareVoted(msg.PeerId) { // 已投票
				continue
			}
			if view != msg.View {
				zlog.Warn("PrepareMsg error, view(%d) != msg.View(%d)", view, msg.View)
				continue
			}
			if !SliceEqual(reqDigest, msg.Digest) {
				zlog.Warn("PrePrepareMsg error, req.digest != msg.Digest")
				continue
			}
			// 验证PrepareMsg
			peer := s.peers[args.Msg.PeerId]
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, peer.pubkey)
			if !ok {
				zlog.Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
				continue
			}
			cert.prepareVote(args)
		}
		// 2f + 1 (包括自身) 后进入 commit 阶段
		if cert.prepareBallot() >= 2*s.f {
			cert.setStage(CommitStage)
			go s.Commit(cert.seq)
		} else {
			break
		}
		fallthrough // 进入后续判断
	case CommitStage:
		argsQ := cert.popAllCommits()
		for _, args := range argsQ {
			msg := args.Msg
			if cert.commitVoted(msg.PeerId) { // 已投票
				continue
			}
			if view != msg.View {
				zlog.Warn("PrepareMsg error, view(%d) != msg.View(%d)", view, msg.View)
				continue
			}
			if !SliceEqual(reqDigest, msg.Digest) {
				zlog.Warn("PrePrepareMsg error, req.digest != msg.Digest")
				continue
			}
			// 验证PrepareMsg
			peer := s.peers[args.Msg.PeerId]
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, peer.pubkey)
			if !ok {
				zlog.Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
				continue
			}
			cert.commitVote(args)
		}
		// 2f + 1 (包括自身) 后进入 reply 阶段
		if cert.commitBallot() >= 2*s.f {
			cert.setStage(ReplyStage)
			s.commitSeqCh <- cert.seq
		}
	}
}

func (s *Server) Reply(seq int64) {
	zlog.Debug("Reply %d", seq)
	req, _, view := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:       view,
		Seq:        seq,
		Timestamp:  time.Now().UnixNano(),
		ClientAddr: req.Req.ClientAddr,
		PeerId:     s.id,
		Result:     req.Req.Command,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.prikey)

	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	var ok bool

	rpcCli, err := rpc.DialHTTP("tcp", req.Req.ClientAddr)
	if err != nil {
		zlog.Warn("dial client %s failed", req.Req.ClientAddr)
		return
	}
	err = rpcCli.Call("Client.ReplyRpc", replyArgs, &ok)
	if err != nil {
		zlog.Warn("rpcCli.Call(\"Client.ReplyRpc\") failed, err:%v", err)
	}
}

func (s *Server) pbft() {
	zlog.Info("== start work loop ==")
	for reqSeq := range s.seqCh {
		s.PrePrepare(reqSeq)
		commitSeq := <-s.commitSeqCh
		if reqSeq != commitSeq {
			zlog.Error("seq != commitSeq")
		}
		go s.Reply(commitSeq)
	}
}
