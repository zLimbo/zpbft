package main

import (
	"flag"
	"zpbft/zpbft"
)

func main() {

	// 解析命令行参数
	var role, maddr, saddr, caddr, pri, pub string
	var f, logLevel int
	flag.StringVar(&role, "role", "", "role of process")
	flag.StringVar(&maddr, "maddr", "", "address of master")
	flag.StringVar(&saddr, "saddr", "", "address of server")
	flag.StringVar(&caddr, "caddr", "", "address of client")
	flag.StringVar(&pri, "pri", "", "private key file")
	flag.StringVar(&pub, "pub", "", "public key file")
	flag.IntVar(&f, "f", 1, "fault tolerance num")
	flag.IntVar(&logLevel, "log", 0, "log level(0:Debug,1:Info,2:Warn,3:Error)")

	flag.Parse()

	switch role {
	case "master":
		// 执行命令: ./naive -role master -maddr localhost:8000 -f 1
		zpbft.RunMaster(maddr, f)
	case "server":
		// 执行命令: ./naive -role server -maddr localhost:8000 -saddr localhost:800x -pri ... -pub ...
		zpbft.RunServer(maddr, saddr, pri, pub)
	case "client":
		// 执行命令: ./naive -role client -maddr localhost:8000 -caddr localhost:9000
		zpbft.RunClient(maddr, caddr)
	default:
		flag.Usage()
	}
}
