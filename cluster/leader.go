package cluster

import (
	"sync"
	"time"
)

const (
	// failedEvalUnblockInterval is the interval at which failed evaluations are
	// unblocked to re-enter the scheduler. A failed evaluation occurs under
	// high contention when the schedulers plan does not make progress.
	failedEvalUnblockInterval = 1 * time.Minute

	// barrierWriteTimeout is used to give Raft a chance to process a
	// possible loss of leadership event if we are unable to get a barrier
	// while leader.
	barrierWriteTimeout = 2 * time.Minute
)

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
func (s *RaftServer) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-s.leaderCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					s.logger.Println("[ERR] raftd: attempted to start the leader loop while running")
					continue
				}

				go func() {
					s.leaderNotify <- isLeader
				}()

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					s.leaderLoop(ch)
				}(weAreLeaderCh)
				s.logger.Println("[INFO] raftd: cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					s.logger.Println("[ERR] raftd: attempted to stop the leader loop while not running")
					continue
				}

				s.logger.Println("[DEBUG] raftd: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				s.logger.Println("[INFO] raftd: cluster leadership lost")
			}

		case <-s.shutdownCh:
			go func() {
				s.shutdowNotify <- true
			}()
			s.logger.Println("[INFO] raftd: shut down")
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run various
// maintenance activities
func (s *RaftServer) leaderLoop(stopCh chan struct{}) {
	establishedLeader := false

RECONCILE:
	// Setup a reconciliation timer
	interval := time.After(s.config.ReconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	barrier := s.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		s.logger.Printf("[ERR] raftd: failed to wait for barrier: %v", err)
		goto WAIT
	}

	// Check if we need to handle initial leadership actions
	if !establishedLeader {
		if err := s.establishLeadership(stopCh); err != nil {
			s.logger.Printf("[ERR] raftd: failed to establish leadership: %v", err)

			// Immediately revoke leadership since we didn't successfully
			// establish leadership.
			if err := s.revokeLeadership(); err != nil {
				s.logger.Printf("[ERR] raftd: failed to revoke leadership: %v", err)
			}

			goto WAIT
		}

		establishedLeader = true
		defer func() {
			if err := s.revokeLeadership(); err != nil {
				s.logger.Printf("[ERR] raftd: failed to revoke leadership: %v", err)
			}
		}()
	}

	// Poll the stop channel to give it priority so we don't waste time
	// trying to perform the other operations if we have been asked to shut
	// down.
	select {
	case <-stopCh:
		return
	default:
	}

WAIT:
	// Wait until leadership is lost
	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		}
	}
}

// establishLeadership is invoked once we become leader and are able
// to invoke an initial barrier. The barrier is used to ensure any
// previously inflight transactions have been committed and that our
// state is up-to-date.
func (s *RaftServer) establishLeadership(stopCh chan struct{}) error {
	// Generate a leader ACL token. This will allow the leader to issue work
	// that requires a valid ACL token.
	return nil
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to a leader.
func (s *RaftServer) revokeLeadership() error {

	// Clear the leader token since we are no longer the leader.

	return nil
}
