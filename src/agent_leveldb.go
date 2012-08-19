/*
 * Copyright (C) Simon Liu
 * Email: simohayha.bobo@gmail.com
 */

package main

import (
	"agent_server"
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	laddr       = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	showVersion = flag.Bool("v", false, "print agent_leveldb's version string")
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `The default for -w is to use the addr from -l, and change the port to 8000.`)
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if *showVersion {
		fmt.Printf("memcached version is %.2f\n", agent_server.Version)
		return
	}

	if *laddr == "" {
		fmt.Fprintln(os.Stderr, "require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	log.SetPrefix("agent_leveldb ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	agent_server.Run_server(*laddr)
}
