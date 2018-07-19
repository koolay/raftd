package cluster

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/koolay/raftd/cluster/config"
)

const (
	defaultDataDir = "/tmp/raft"
	raftState      = "state/"
	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

type RaftServer struct {
	config    *config.ServerConfiguration
	logger    *log.Logger
	leaderCh  <-chan bool
	raft      *raft.Raft
	raftStore *raftboltdb.BoltStore
	raftInmem *raft.InmemStore
	// fsm is the state machine used with Raft
	fsm *SimpleFSM

	// leaderAcl is the management ACL token that is valid when resolved by the
	// current leader.
	leaderAcl     string
	leaderAclLock sync.Mutex

	leaderNotify  chan<- bool
	shutdowNotify chan<- bool

	shutdownCh chan struct{}
	shutdown   bool
}

func NewRaftServer(logger *log.Logger, config *config.ServerConfiguration,
	leaderNotify chan<- bool, shutdownNotify chan<- bool) (*RaftServer, error) {

	s := &RaftServer{
		logger:        logger,
		config:        config,
		leaderNotify:  leaderNotify,
		shutdowNotify: shutdownNotify,
	}
	if s.config.DataDir == "" {
		s.config.DataDir = defaultDataDir
	}

	err := s.setupRaft()
	if err != nil {
		return nil, err
	}
	go s.monitorLeadership()
	return s, nil
}

func (s *RaftServer) Join(node *Node, voter bool) error {

	serverID := raft.ServerID(node.ID)
	address := raft.ServerAddress(node.Addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(node.ID) || srv.Address == address {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == address && srv.ID == serverID {
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
		}
	}

	if voter {
		f := s.raft.AddVoter(serverID, address, 0, 0)
		if err := f.Error(); err != nil {
			return f.Error()
		}
	} else {
		f := s.raft.AddNonvoter(serverID, address, 0, 0)
		if err := f.Error(); err != nil {
			return f.Error()
		}
	}

	s.logger.Println("node joined successfully")
	return nil
}

func (s *RaftServer) GetLeader() string {
	return string(s.raft.Leader())
}

func (s *RaftServer) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *RaftServer) initRaftConfig() {
	s.config.RaftConfig = &raft.Config{
		Logger:             s.logger,
		ProtocolVersion:    3,
		HeartbeatTimeout:   2 * time.Second,
		ElectionTimeout:    3 * time.Second,
		CommitTimeout:      3 * time.Second,
		MaxAppendEntries:   3,
		SnapshotInterval:   5 * time.Second,
		LeaderLeaseTimeout: 1 * time.Second,
	}
}

func (s *RaftServer) setupRaft() error {

	s.initRaftConfig()
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				s.logger.Printf("[ERR] raftd: failed to close Raft store: %v", err)
			}
		}
	}()
	// Create the base raft path
	dataDir := filepath.Join(s.config.DataDir, raftState)
	if err := ensurePath(dataDir, true); err != nil {
		return err
	}

	var err error
	s.fsm, err = NewFSM(dataDir)
	if err != nil {
		return fmt.Errorf("Failed to init FSM. dir: %s, error: %s", dataDir, err.Error())
	}

	addr, err := net.ResolveTCPAddr("tcp", s.config.RaftAddr)
	if err != nil {
		return err
	}

	trans, err := raft.NewTCPTransport(s.config.RaftAddr, addr, 3, s.config.RaftTimeout, os.Stderr)
	if err != nil {
		return err
	}

	// Make sure we set the LogOutput.
	s.config.RaftConfig.LogOutput = s.config.LogOutput
	s.config.RaftConfig.LocalID = raft.ServerID(s.config.NodeID)

	// Build an all in-memory setup for dev mode, otherwise prepare a full
	// disk-based setup.
	var log raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	if s.config.DevMode {
		store := raft.NewInmemStore()
		s.raftInmem = store
		stable = store
		log = store
		snap = raft.NewDiscardSnapshotStore()

	} else {

		// Create the BoltDB backend
		store, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
		if err != nil {
			return err
		}
		s.raftStore = store
		stable = store

		// Wrap the store in a LogCache to improve performance
		cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
		if err != nil {
			store.Close()
			return err
		}
		log = cacheStore

		// Create the snapshot store
		snapshots, err := raft.NewFileSnapshotStore(dataDir, snapshotsRetained, s.config.LogOutput)
		if err != nil {
			if s.raftStore != nil {
				s.raftStore.Close()
			}
			return err
		}
		snap = snapshots

		// For an existing cluster being upgraded to the new version of
		// Raft, we almost never want to run recovery based on the old
		// peers.json file. We create a peers.info file with a helpful
		// note about where peers.json went, and use that as a sentinel
		// to avoid ingesting the old one that first time (if we have to
		// create the peers.info file because it's not there, we also
		// blow away any existing peers.json file).
		peersFile := filepath.Join(dataDir, "peers.json")
		peersInfoFile := filepath.Join(dataDir, "peers.info")
		if _, err := os.Stat(peersInfoFile); os.IsNotExist(err) {
			if err := ioutil.WriteFile(peersInfoFile, []byte(peersInfoContent), 0755); err != nil {
				return fmt.Errorf("failed to write peers.info file: %v", err)
			}

			// Blow away the peers.json file if present, since the
			// peers.info sentinel wasn't there.
			if _, err := os.Stat(peersFile); err == nil {
				if err := os.Remove(peersFile); err != nil {
					return fmt.Errorf("failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
				}
				s.logger.Printf("[INFO] raftd: deleted peers.json file (see peers.info for details)")
			}
		} else if _, err := os.Stat(peersFile); err == nil {
			s.logger.Printf("[INFO] raftd: found peers.json file, recovering Raft configuration...")
			configuration, err := raft.ReadPeersJSON(peersFile)
			if err != nil {
				return fmt.Errorf("recovery failed to parse peers.json: %v", err)
			}

			tmpFsm, err := NewFSM(dataDir)
			if err != nil {
				return fmt.Errorf("Faild to new FSM. error: %s", err.Error())
			}
			if err := raft.RecoverCluster(s.config.RaftConfig, tmpFsm,
				log, stable, snap, trans, configuration); err != nil {
				return fmt.Errorf("recovery failed: %v", err)
			}
			if err := os.Remove(peersFile); err != nil {
				return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
			}
			s.logger.Printf("[INFO] raftd: deleted peers.json file after successful recovery")
		}
	}

	// If we are in bootstrap or dev mode and the state is clean then we can
	// bootstrap now.
	if s.config.Bootstrap || s.config.DevMode {
		hasState, err := raft.HasExistingState(log, stable, snap)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      s.config.RaftConfig.LocalID,
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(s.config.RaftConfig,
				log, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	// Setup the leader channel
	leaderCh := make(chan bool, 1)
	s.config.RaftConfig.NotifyCh = leaderCh
	s.leaderCh = leaderCh

	// Setup the Raft store
	s.raft, err = raft.NewRaft(s.config.RaftConfig, s.fsm, log, stable, snap, trans)
	if err != nil {
		return err
	}
	return nil
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// peersInfoContent is used to help operators understand what happened to the
// peers.json file. This is written to a file called peers.info in the same
// location.
const peersInfoContent = `
As of Nomad 0.5.5, the peers.json file is only used for recovery
after an outage. It should be formatted as a JSON array containing the address
and port (RPC) of each Nomad server in the cluster, like this:
["10.1.0.1:4647","10.1.0.2:4647","10.1.0.3:4647"]
Under normal operation, the peers.json file will not be present.
When Nomad starts for the first time, it will create this peers.info file and
delete any existing peers.json file so that recovery doesn't occur on the first
startup.
Once this peers.info file is present, any peers.json file will be ingested at
startup, and will set the Raft peer configuration manually to recover from an
outage. It's crucial that all servers in the cluster are shut down before
creating the peers.json file, and that all servers receive the same
configuration. Once the peers.json file is successfully ingested and applied, it
will be deleted.
Please see https://www.nomadproject.io/guides/outage.html for more information.
`
