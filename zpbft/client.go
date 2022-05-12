package main

import (
	"net/rpc"
	"zpbft/zlog"
)

type Client struct {
	maddr string
	addr  string
	peers map[int32]*Peer
	f     int
}

func RunClient(maddr, caddr string) {

	c := &Client{
		maddr: maddr,
		addr:  caddr,
		peers: make(map[int32]*Peer),
	}

	// 从 master 那里获取peers信息
	c.getPeersFromMaster()

	c.start()
}

func (c *Client) getPeersFromMaster() {
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", c.maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, err:%v", err)
	}

	args := &GetPeersArgs{}
	reply := &GetPeersReply{}
	rpcCli.Call("Master.GetPeersRpc", args, reply)

	// 设置节点信息
	for i := range reply.Addrs {
		c.peers[int32(i)] = &Peer{
			id:     int32(i),
			addr:   reply.Addrs[i],
			pubkey: reply.Pubkeys[i],
		}
	}

	// n = 3f + 1
	c.f = (len(c.peers) - 1) / 3
}

func (c *Client) start() {
	for {

	}
}
