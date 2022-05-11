package main

import (
	"net/http"
	"net/rpc"
	"sync"
)

const PeerNum = 4

type Master struct {
	host2pubkey map[string][]byte
	mu          sync.Mutex
	cond        sync.Cond
}

func RunMaster(mhost string) {
	master := &Master{
		host2pubkey: map[string][]byte{},
	}
	master.cond = sync.Cond{L: &master.mu}
	// 开启 rpc server 监听
	rpc.Register(master)
	rpc.HandleHTTP()
	err := http.ListenAndServe(mhost, nil)
	if err != nil {
		Error("http.ListenAndServe failed, err:%v", err)
	}
}

type RegisterArgs struct {
	Host   string
	Pubkey []byte
}

type RegisterReply struct {
	Host2pubkey map[string][]byte
	Ok          bool
}

func (m *Master) RegisterRpc(args *RegisterArgs, reply *RegisterReply) error {

	m.mu.Lock()
	if len(m.host2pubkey) >= PeerNum {
		m.mu.Unlock()
		reply.Ok = false
		return nil
	}
	Info("server host:%s", args.Host)
	m.host2pubkey[args.Host] = args.Pubkey
	m.mu.Unlock()

	m.cond.L.Lock()
	for len(m.host2pubkey) < PeerNum {
		m.cond.Wait()
	}
	m.cond.L.Unlock()
	m.cond.Broadcast()

	reply.Host2pubkey = m.host2pubkey
	reply.Ok = true
	return nil
}
