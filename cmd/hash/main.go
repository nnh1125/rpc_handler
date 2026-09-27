package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"rpc_handler/client"
	"rpc_handler/internal/operations"
	"time"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "RPC server address")
	data := flag.String("data", "abc", "text to hash remotely")
	timeout := flag.Duration("timeout", 5*time.Second, "total connection and call timeout")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c, err := client.Dial(ctx, *address)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	var result operations.HashResult
	if err := c.Call(ctx, "hash_compute", operations.HashParams{Data: []byte(*data)}, &result); err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Hex)
}
