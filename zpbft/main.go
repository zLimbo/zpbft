package main

import "flag"

func main() {

	var role, mhost, host, pri, pub string
	flag.StringVar(&role, "role", "", "")
	flag.StringVar(&mhost, "mhost", "", "")
	flag.StringVar(&host, "host", "", "")
	flag.StringVar(&pri, "pri", "", "")
	flag.StringVar(&pub, "pub", "", "")

	flag.Parse()

	Info("role:%s, mhost:%s, host:%s", role, mhost, host)
	switch role {
	case "master":
		RunMaster(mhost)
	case "server":
		RunServer(mhost, host, pri, pub)
	case "client":
		// RunClient(mhost, host, pri, pub)
	}
}
