package main

import "flag"

func main() {

	var role, maddr, saddr, caddr, pri, pub string
	var f, logLevel int
	flag.StringVar(&role, "role", "", "role of process")
	flag.StringVar(&maddr, "maddr", "", "address of master")
	flag.StringVar(&saddr, "addr", "", "address of server")
	flag.StringVar(&caddr, "addr", "", "address of client")
	flag.StringVar(&pri, "pri", "", "private key file")
	flag.StringVar(&pub, "pub", "", "public key file")
	flag.IntVar(&f, "f", 1, "fault tolerance num")
	flag.IntVar(&logLevel, "log", 0, "log level(0:Debug,1:Info,2:Warn,3:Error)")

	flag.Parse()

	switch role {
	case "master":
		RunMaster(maddr, f)
	case "server":
		RunServer(maddr, saddr, pri, pub)
	case "client":
		RunClient(maddr, caddr)
	}
	
}
