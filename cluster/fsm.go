package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

var (
	bucket = []byte("fsm_state")
)

type fsmSnapshot struct {
	kvMap map[string]string
}

type SimpleFSM struct {
	db *bolt.DB
	mu sync.Mutex
}

func NewFSM(dataDir string) (*SimpleFSM, error) {
	// Create the BoltDB backend
	db, err := bolt.Open(filepath.Join(dataDir, "fsm.db"), 0600, nil)
	if err != nil {
		panic(err)
		return nil, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("Failed to begin FSM db. %s", err.Error())
	}
	defer tx.Rollback()
	// Use the transaction...
	_, err = tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &SimpleFSM{
		db: db,
	}, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (s *SimpleFSM) Apply(log *raft.Log) interface{} {
	fmt.Println("FSM: apply")
	return nil
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (s *SimpleFSM) Snapshot() (raft.FSMSnapshot, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clone the kvstore into a map for easy transport
	mapClone := make(map[string]string)

	s.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(bucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			mapClone[string(k[:])] = string(v[:])
		}

		return nil
	})
	return &fsmSnapshot{kvMap: mapClone}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (s *SimpleFSM) Restore(kvMap io.ReadCloser) error {

	kvSnapshot := make(map[string]string)
	if err := json.NewDecoder(kvMap).Decode(&kvSnapshot); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	for k, v := range kvSnapshot {
		s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucket)
			err := b.Put([]byte(k), []byte(v))
			return err
		})
	}
	return nil
}
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.kvMap)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
