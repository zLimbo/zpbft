package main

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
)

const (
	MBSize       = 1024 * 1024
	ChanSize     = 10000
	KConfigFile  = "./config/config.json"
	KCertsDir    = "./certs"
	KLocalIpFile = "./config/local_ip.txt"
)

type Node struct {
	id     int64
	ip     string
	port   int
	addr   string
	priKey []byte
	pubKey []byte
}

func NewNode(ip string, port int, priKey, pubKey []byte) *Node {
	id := GetId(ip, port)
	node := &Node{id, ip, port, ip + ":" + strconv.Itoa(port), priKey, pubKey}
	return node
}

type Config struct {
	ServerHosts []string `json:ServerHosts`
	ClientHost  string   `json:ClientHost`
	LogLevel    LogLevel `json:LogLevel`

	Id2Node    map[int64]*Node
	ClientNode *Node
	PeerIds    []int64
	LocalIp    string
	FalultNum  int
	RouteMap   map[int64][]int64
}

var KConfig Config

func InitConfig() {

	// 读取 json, 反序列化到 KConfig
	jsonBytes, err := ioutil.ReadFile(KConfigFile)
	if err != nil {
		Error("read %s failed.", KConfigFile)
	}
	Debug("config: ", string(jsonBytes))
	err = json.Unmarshal(jsonBytes, &KConfig)
	if err != nil {
		Error("json.Unmarshal(jsonBytes, &KConfig) err: %v", err)
	}

	// 配置节点ip, port, 公私钥
	// KConfig.PeerIps = KConfig.PeerIps[:KConfig.IpNum]
	// KConfig.Id2Node = make(map[int64]*Node)
	// for i := 0; i < KConfig.ProcessNum; i++ {
	// 	for _, ip := range KConfig.PeerIps {
	// 		port := KConfig.PortBase + 1 + i
	// 		id := GetId(ip, port)
	// 		keyDir := KCertsDir + "/" + fmt.Sprint(id)
	// 		priKey, pubKey := ReadKeyPair(keyDir)
	// 		KConfig.Id2Node[id] = NewNode(ip, port, priKey, pubKey)
	// 		KConfig.PeerIds = append(KConfig.PeerIds, id)
	// 	}
	// }

	// // 计算容错数
	// KConfig.FalultNum = (len(KConfig.Id2Node) - 1) / 3

	// // 设置本地IP和客户端
	// id := GetId(KConfig.ClientIp, KConfig.PortBase+1)
	// keyDir := KCertsDir + "/" + fmt.Sprint(id)
	// priKey, pubKey := ReadKeyPair(keyDir)
	// KConfig.ClientNode = NewNode(KConfig.ClientIp, KConfig.PortBase-1, priKey, pubKey)
	// KConfig.LocalIp = GetLocalIp()
}

func GetNode(id int64) *Node {

	if id == KConfig.ClientNode.id {
		return KConfig.ClientNode
	}
	node, ok := KConfig.Id2Node[id]
	if !ok {
		Error("The node of this ID(%d) does not exist!", id)
	}
	return node
}

func GetIndex(nodeId int64) int {
	for idx, id := range KConfig.PeerIds {
		if nodeId == id {
			return idx
		}
	}
	return -1
}
