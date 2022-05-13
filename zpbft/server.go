package zpbft

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"zpbft/zkv"
	"zpbft/zlog"
)

type Peer struct {
	id     int32
	addr   string
	pubkey []byte
	rpcCli *rpc.Client
}

type Server struct {
	mu          sync.Mutex
	f           int
	id          int32
	addr        string
	prikey      []byte
	pubkey      []byte
	maddr       string
	peers       map[int32]*Peer
	leader      int32
	view        int32
	maxSeq      int64
	seqCh       chan int64
	commitSeqCh chan int64
	seq2cert    map[int64]*LogCert
	zkv         *zkv.ZKV
}

func RunServer(maddr, saddr, pri, pub string) {

	prikey, pubkey := readKeyPair(pri, pub)
	s := &Server{
		addr:        saddr,
		prikey:      prikey,
		pubkey:      pubkey,
		maddr:       maddr,
		peers:       map[int32]*Peer{},
		seqCh:       make(chan int64, 100),
		commitSeqCh: make(chan int64, 100),
		seq2cert:    make(map[int64]*LogCert),
		zkv:         zkv.NewZKV(),
	}

	// 开启rpc服务
	s.server()

	// 注册节点信息，并获取其他节点信息
	s.register()

	// 并行建立连接
	s.connectPeers()

	// 进行pbft服务
	s.pbft()
}

func (s *Server) server() {
	// 放入协程防止阻塞后面函数
	go func() {
		rpc.Register(s)
		rpc.HandleHTTP()
		err := http.ListenAndServe(s.addr, nil)
		if err != nil {
			zlog.Error("http.ListenAndServe failed, err:%v", err)
		}
	}()
}

func (s *Server) register() {
	time.Sleep(500 * time.Millisecond)
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", s.maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, err:%v", err)
	}

	args := &RegisterArgs{
		Addr:   s.addr,
		Pubkey: s.pubkey,
	}
	reply := &RegisterReply{}

	zlog.Info("register peer info ...")
	rpcCli.Call("Master.RegisterRpc", args, reply)

	// 重复addr注册或超过 PeerNum 限定节点数，注册失败
	if !reply.Ok {
		zlog.Error("register failed")
	}

	// 设置节点信息
	for i := range reply.Addrs {
		if reply.Addrs[i] == s.addr {
			s.id = int32(i)
			continue
		}
		s.peers[int32(i)] = &Peer{
			id:     int32(i),
			addr:   reply.Addrs[i],
			pubkey: reply.Pubkeys[i],
		}
	}

	// n = 3f + 1
	s.f = (len(reply.Addrs) - 1) / 3
	s.leader = 0
}

func (s *Server) connectPeers() {
	zlog.Info("build connect with other peers ...")
	wg := sync.WaitGroup{}
	wg.Add(len(s.peers))
	for _, peer := range s.peers {
		p := peer
		go func() {
			// 每隔1s请求建立连接，10s未连接则报错
			t0 := time.Now()
			for time.Since(t0).Seconds() < 10 {
				rpcCli, err := rpc.DialHTTP("tcp", p.addr)
				if err == nil {
					zlog.Debug("dial (id=%d,addr=%s) success", p.id, p.addr)
					p.rpcCli = rpcCli
					wg.Done()
					return
				}
				zlog.Warn("dial (id=%d,addr=%s) error, err:%v", p.id, p.addr, err)
				time.Sleep(time.Second)
			}
			zlog.Error("connect (id=%d,addr=%s) failed, terminate", p.id, p.addr)
		}()
	}
	wg.Wait()
	zlog.Info("==== connect all peers success ====")
}

func readKeyPair(pri, pub string) ([]byte, []byte) {
	prikey, err := ioutil.ReadFile(pri)
	if err != nil {
		zlog.Error("ioutil.ReadFile(pri) filed, err:%v", err)
	}
	pubkey, err := ioutil.ReadFile(pub)
	if err != nil {
		zlog.Error("ioutil.ReadFile(pub) filed, err:%v", err)
	}
	return prikey, pubkey
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {

	// 如果不是leader，将请求转发至leader
	if s.leader != s.id {
		return s.peers[s.leader].rpcCli.Call("Server.RequestRpc", args, reply)
	}

	// leader 放入请求队列直接返回，后续异步通知客户端
	zlog.Info("req from: %s", args.Req.ClientAddr)

	// 客户端的请求暂不验证

	// leader 分配seq
	seq := s.assignSeq()
	digest := Digest(args.Req)
	s.getCertOrNew(seq).set(args, digest, s.view)

	go func() {
		// 使用协程：因为通道会阻塞
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
	zlog.Debug("PrePrepare %d", seq)
	req, digest, view := s.getCertOrNew(seq).get()
	msg := &PrePrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	digest = Digest(msg)
	sign := Sign(digest, s.prikey)
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
			reply := &PrePrepareReply{}
			err := p.rpcCli.Call("Server.PrePrepareRpc", args, reply)
			if err != nil {
				zlog.Warn("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
			// 等待发完2f个节点再进入下一阶段

			if atomic.AddInt32(&count, 1) == 2*int32(s.f) {
				zlog.Debug("PrePrepare %d ok", seq)
				s.Prepare(seq)
			}
		}()
	}

}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *PrePrepareReply) error {
	msg := args.Msg
	zlog.Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)
	// 预设返回失败
	reply.Ok = false

	// 验证PrePrepareMsg
	peer := s.peers[args.Msg.PeerId]
	digest := Digest(msg)
	ok := Verify(digest, args.Sign, peer.pubkey)
	if !ok {
		zlog.Warn("PrePrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
		return nil
	}

	// 验证RequestMsg
	reqArgs := args.ReqArgs
	digest = Digest(reqArgs.Req)
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
	go s.verifyBallot(cert)

	// 返回成功
	reply.Ok = true

	return nil
}

func (s *Server) Prepare(seq int64) {
	zlog.Debug("Prepare %d", seq)
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	// 配置rpc参数,相比PrePrepare无需req
	args := &PrepareArgs{
		Msg:  msg,
		Sign: Sign(Digest(msg), s.prikey),
	}
	// prepare广播
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		go func() {
			reply := &PrepareReply{}
			err := p.rpcCli.Call("Server.PrepareRpc", args, reply)
			if err != nil {
				zlog.Warn("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
		}()
	}
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *PrepareReply) error {
	msg := args.Msg
	zlog.Debug("PrepareRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushPrepare(args)

	// 尝试计票
	go s.verifyBallot(cert)

	reply.Ok = true
	return nil
}

func (s *Server) Commit(seq int64) {
	zlog.Debug("Commit %d", seq)
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &CommitMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		PeerId: s.id,
	}
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: Sign(Digest(msg), s.prikey),
	}
	// commit广播
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		go func() {
			reply := &CommitReply{}
			err := p.rpcCli.Call("Server.CommitRpc", args, reply)
			if err != nil {
				zlog.Warn("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
		}()
	}
}

func (s *Server) CommitRpc(args *CommitArgs, reply *CommitReply) error {
	msg := args.Msg
	zlog.Debug("CommitRpc, seq: %d, from: %d", msg.Seq, msg.PeerId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushCommit(args)

	// 尝试计票
	go s.verifyBallot(cert)

	reply.Ok = true
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	req, reqDigest, view := cert.get()

	// req 为空则不进行后续阶段
	if req == nil {
		zlog.Debug("req is nil")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
			digest := Digest(msg)
			ok := Verify(digest, args.Sign, peer.pubkey)
			if !ok {
				zlog.Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
				continue
			}
			cert.prepareVote(args)
		}
		zlog.Debug("verifyBallot, seq: %d, prepare: %d, commit: %d, stage: %d",
			cert.seq, cert.prepareBallot(), cert.commitBallot(), cert.getStage())
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
			digest := Digest(msg)
			ok := Verify(digest, args.Sign, peer.pubkey)
			if !ok {
				zlog.Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
				continue
			}
			cert.commitVote(args)
		}
		zlog.Debug("verifyBallot, seq: %d, prepare: %d, commit: %d, stage: %d",
			cert.seq, cert.prepareBallot(), cert.commitBallot(), cert.getStage())
		// 2f + 1 (包括自身) 后进入 apply和reply 阶段
		if cert.commitBallot() >= 2*s.f {
			cert.setStage(ReplyStage)
			// if s.id == s.leader {
			// 	zlog.Info("commit %d", cert.seq)
			// 	s.commitSeqCh <- cert.seq
			// }
			go s.Apply(cert.seq)
		}
	}
}

// apply 日志，执行完后返回
func (s *Server) Apply(seq int64) {
	cert := s.getCertOrNew(seq)
	cmd := cert.req.Req.Command
	zlog.Info("execute cmd: [%s]", cmd)
	result := s.zkv.Execute(cmd)
	go s.Reply(seq, result)

	time.Sleep(100 * time.Millisecond)
	all := s.zkv.All()
	fmt.Println("\n==== zkv ====")
	fmt.Printf("%s", all)
	fmt.Println("==== zkv ====")
	fmt.Println()
}

func (s *Server) Reply(seq int64, result string) {
	zlog.Debug("Reply %d", seq)
	req, _, view := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:       view,
		Seq:        seq,
		Timestamp:  time.Now().UnixNano(),
		ClientAddr: req.Req.ClientAddr,
		PeerId:     s.id,
		Result:     result,
	}
	digest := Digest(msg)
	sign := Sign(digest, s.prikey)

	rpcCli, err := rpc.DialHTTP("tcp", req.Req.ClientAddr)
	if err != nil {
		zlog.Warn("dial client %s failed", req.Req.ClientAddr)
		return
	}

	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	reply := &ReplyReply{}

	err = rpcCli.Call("Client.ReplyRpc", replyArgs, reply)
	if err != nil {
		zlog.Warn("rpcCli.Call(\"Client.ReplyRpc\") failed, %v", err)
	}
}

func (s *Server) pbft() {
	zlog.Info("== start work loop ==")
	// 串行处理请求
	for reqSeq := range s.seqCh {
		s.PrePrepare(reqSeq)
	}
}
