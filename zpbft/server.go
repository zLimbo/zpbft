package main

import (
	"io/ioutil"
	"net/http"
	"net/rpc"
	"sync"
	"time"
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
}

func RunServer(maddr, saddr, pri, pub string) {

	prikey, pubkey := readKeyPair(pri, pub)
	s := &Server{
		addr:        saddr,
		prikey:      prikey,
		pubkey:      pubkey,
		maddr:       maddr,
		peers:       map[int32]*Peer{},
		seqCh:       make(chan int64),
		commitSeqCh: make(chan int64),
		seq2cert:    make(map[int64]*LogCert),
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
			continue
		}
		s.peers[int32(i)] = &Peer{
			id:     int32(i),
			addr:   reply.Addrs[i],
			pubkey: reply.Pubkeys[i],
		}
	}

	// n = 3f + 1
	s.f = (len(s.peers) - 1) / 3
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
