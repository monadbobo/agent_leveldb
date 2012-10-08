/*
 * Copyright (C) Simon Liu
 * Email: simohayha.bobo@gmail.com
 */

package main

import (
	"agent_server"
	"code.google.com/p/vitess/go/relog"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

var (
	laddr       = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	showVersion = flag.Bool("v", false, "print agent_leveldb's version string")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	configFile  = flag.String("config", "", "config file name (json format)")
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
		fmt.Printf("agent_leveldb version is %.2f\n", agent_server.Version)
		return
	}

	agent_server.Init("agent_leveldb")
	if *configFile != "" {
		agent_server.Parse_config(*configFile)
	}

	if *laddr == "" {
		fmt.Fprintln(os.Stderr, "require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			relog.Fatal("%s", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	signal_init()

	agent_server.Run_server(*laddr)
}

func signal_init() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				relog.Warning("Terminating on signal", sig)
				if *cpuprofile != "" {
					pprof.StopCPUProfile()
				}
				os.Exit(0)
			}
		}
	}()
}
