package config

import (
	"io"
	"time"

	"github.com/hashicorp/raft"
)

type ServerConfiguration struct {
	DevMode           bool
	RaftTimeout       time.Duration
	LogOutput         io.Writer
	RaftConfig        *raft.Config
	NodeID            string
	DataDir           string
	Bootstrap         bool
	ReconcileInterval time.Duration
	RaftAddr          string
}

// type RaftConfiguration struct {
// 	LogOutput       io.Writer
// 	LocalID         raft.ServerID
// 	ProtocolVersion int
// }
