package zkv

import (
	"fmt"
	"strings"
)

const usage = "gramma error, usage: get <key>, set <key> <val>, del <key>, all"

type ZKV struct {
	db map[string]string
}

func (z *ZKV) Put(key, val string) {
	z.db[key] = val
}

func (z *ZKV) Get(key string) (string, bool) {
	val, ok := z.db[key]
	return val, ok
}

func (z *ZKV) Del(key string) {
	delete(z.db, key)
}

func (z *ZKV) Execute(cmd string) bool {
	words := strings.Split(cmd, " ")
	op := strings.ToLower(words[0])
	switch op {
	case "get":
		if len(words) != 2 {
			fmt.Println(usage)
			return false
		}
		key := words[1]
		val, ok := z.Get(key)
		if !ok {
			fmt.Printf("error: can't find this key <%s>\n", key)
			return false
		}
		fmt.Printf("result: %s\n", val)
	case "put":
		if len(words) != 3 {
			fmt.Println(usage)
			return false
		}
		key, val := words[1], words[2]
		z.Put(key, val)
		fmt.Println("put ok")
	case "del":
		if len(words) != 2 {
			fmt.Println(usage)
			return false
		}
		key := words[1]
		z.Del(key)
		fmt.Println("del ok")
	case "all":
		if len(words) != 1 {
			fmt.Println(usage)
			return false
		}
		for key, val := range z.db {
			fmt.Printf("%s:%s\n", key, val)
		}
	default:
		fmt.Printf("syntax error, usage: %s\n", usage)
		return false
	}
	return true
}
