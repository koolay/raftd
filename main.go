package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/koolay/raftd/cluster/helper"

	"github.com/koolay/raftd/cluster"
	"github.com/koolay/raftd/cluster/config"
)

func main() {

	var join string
	var nodeID string
	var bind string
	var dataDir string

	flag.StringVar(&nodeID, "node", "", "id of node")
	flag.StringVar(&join, "join", "", "if join the cluster")
	flag.StringVar(&bind, "bind", "127.0.0.1:7600", "bind for cluster listen")
	flag.StringVar(&dataDir, "data", "data", "data dir")
	flag.Parse()

	var err error

	if nodeID == "" {
		nodeID, err = helper.GenerateNodeID(bind)
		if err != nil {
			panic(err)
		}
	}
	logger := log.New(os.Stdout, "", 0)
	logoutput := os.Stdout
	cfg := &config.ServerConfiguration{
		DevMode:           true,
		RaftTimeout:       10 * time.Second,
		LogOutput:         logoutput,
		NodeID:            nodeID,
		DataDir:           dataDir,
		Bootstrap:         true,
		ReconcileInterval: 5 * time.Second,
		RaftAddr:          bind,
	}
	s, err := cluster.NewRaftServer(logger, cfg)
	if err != nil {
		fmt.Printf("Failed to init raft server. error: %s \n", err.Error())
		os.Exit(1)
	}
	if join != "" {
		var joinID string
		joinID, err := helper.GenerateNodeID(join)
		if err != nil {
			panic(err)
		}
		go func() {
			er := s.Join(&cluster.Node{
				ID:   joinID,
				Addr: join,
			}, false)
			if er != nil {
				fmt.Println(er)
			}
			// time.Sleep(1 * time.Second)
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	tc := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-tc.C:
			fmt.Println("I'm leader. ", s.GetLeader(), s.IsLeader())
		case <-sigCh:
			fmt.Println("quit")
			os.Exit(1)
		}
	}

}
