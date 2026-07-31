package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"time"
)

const (
	connDeadline = 2 * time.Second
	probeTimeout = 50 * time.Millisecond

	// checkpointInterval is how often the WAL is truncated. See checkpointLoop
	// for why it is long rather than eager.
	checkpointInterval = 10 * time.Minute
)

// Serve listens on sockPath and dispatches envelopes to the state handlers.
// It returns when ctx is cancelled or the listener errors. On return the
// socket file is removed.
//
// If the initial bind fails, Serve probes the existing socket: a live daemon
// (one that answers ping) is left alone and Serve returns an error; a stale
// file (no answer within probeTimeout) is removed and bind is retried once.
func Serve(ctx context.Context, state *State, sockPath string) error {
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		if isLiveSocket(sockPath, probeTimeout) {
			return fmt.Errorf("daemon already running at %s", sockPath)
		}
		if rmErr := os.Remove(sockPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("remove stale socket %s: %w", sockPath, rmErr)
		}
		l, err = net.Listen("unix", sockPath)
		if err != nil {
			return fmt.Errorf("listen %s: %w", sockPath, err)
		}
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		l.Close()
		os.Remove(sockPath)
		return fmt.Errorf("chmod %s: %w", sockPath, err)
	}

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	go checkpointLoop(ctx, state)

	defer os.Remove(sockPath)

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		go handle(conn, state)
	}
}

// isLiveSocket reports whether sockPath has a daemon currently answering ping
// within timeout. Any failure (dial error, no/garbled response, Pong=false) is
// treated as not-live so the caller can clear a stale file and rebind.
func isLiveSocket(sockPath string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("unix", sockPath, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if err := json.NewEncoder(conn).Encode(Envelope{Type: "ping"}); err != nil {
		return false
	}
	var resp PingResp
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return false
	}
	return resp.Pong
}

// checkpointLoop truncates the write-ahead log periodically, since SQLite's own
// checkpointing bounds the WAL's size but never reduces it. See State.CheckpointWAL.
//
// The interval is long on purpose. Reclaiming disk is not urgent, and a
// TRUNCATE checkpoint has to wait out readers, so doing it often would trade a
// real cost against a benefit measured in megabytes per day.
func checkpointLoop(ctx context.Context, state *State) {
	t := time.NewTicker(checkpointInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Busy is the expected outcome when a reader is mid-snapshot, and the
			// next tick will retry, so it is not worth reporting on its own. Any
			// error is logged rather than swallowed: a checkpoint that fails every
			// time means the WAL grows forever, and silence would hide that.
			if err := state.CheckpointWAL(); err != nil {
				fmt.Fprintf(os.Stderr, "deja daemon: wal checkpoint: %v\n", err)
			}
		}
	}
}

func handle(conn net.Conn, state *State) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "deja daemon: connection panic: %v\n%s", r, debug.Stack())
		}
	}()
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(connDeadline))

	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return
	}

	switch env.Type {
	case "suggest":
		var req SuggestReq
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			return
		}
		_ = enc.Encode(state.Suggest(req, time.Now()))

	case "record":
		var req RecordReq
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			return
		}
		if err := state.Record(req); err != nil {
			fmt.Fprintf(os.Stderr, "deja daemon: record: %v\n", err)
		}
		_ = enc.Encode(RecordResp{})

	case "ping":
		_ = enc.Encode(state.Ping())

	case "setconfig":
		var req SetConfigReq
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			return
		}
		_ = enc.Encode(state.SetConfig(req))

	case "getconfig":
		_ = enc.Encode(state.GetConfig())
	}
}
