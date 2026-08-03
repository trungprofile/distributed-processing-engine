// Package cluster holds the small amount of shared state the control plane
// needs: which nodes are alive, what they are doing, and how to ask one to
// drain. Redis is already a hard dependency, so it doubles as the registry
// rather than adding a second coordination system.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// NodeState is one worker's self-reported status.
type NodeState struct {
	NodeID    string `json:"node_id"`
	Capacity  int32  `json:"capacity"`
	InFlight  int64  `json:"in_flight"`
	Processed int64  `json:"processed"`
	Reclaimed int64  `json:"reclaimed"`
	Draining  bool   `json:"draining"`
	UpdatedAt int64  `json:"updated_at"`
}

// Registry tracks live nodes and carries drain requests.
type Registry struct {
	rdb        redis.UniversalClient
	hashKey    string
	drainChan  string
	StaleAfter time.Duration
}

// NewRegistry namespaces a registry under prefix (default "dpe").
func NewRegistry(rdb redis.UniversalClient, prefix string) *Registry {
	if prefix == "" {
		prefix = "dpe"
	}
	return &Registry{
		rdb:        rdb,
		hashKey:    prefix + ":nodes",
		drainChan:  prefix + ":control:drain",
		StaleAfter: 15 * time.Second,
	}
}

// Heartbeat publishes this node's state. Workers call it on a short ticker;
// the timestamp is what lets List evict nodes that died without deregistering.
func (r *Registry) Heartbeat(ctx context.Context, st NodeState) error {
	st.UpdatedAt = time.Now().Unix()
	blob, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal node state: %w", err)
	}
	return r.rdb.HSet(ctx, r.hashKey, st.NodeID, blob).Err()
}

// Deregister removes a node that shut down cleanly.
func (r *Registry) Deregister(ctx context.Context, nodeID string) error {
	return r.rdb.HDel(ctx, r.hashKey, nodeID).Err()
}

// List returns live nodes, pruning entries whose heartbeat has gone stale.
func (r *Registry) List(ctx context.Context) ([]NodeState, error) {
	raw, err := r.rdb.HGetAll(ctx, r.hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	cutoff := time.Now().Add(-r.StaleAfter).Unix()
	out := make([]NodeState, 0, len(raw))
	var stale []string
	for id, blob := range raw {
		var st NodeState
		if err := json.Unmarshal([]byte(blob), &st); err != nil {
			stale = append(stale, id)
			continue
		}
		if st.UpdatedAt < cutoff {
			stale = append(stale, id)
			continue
		}
		out = append(out, st)
	}
	if len(stale) > 0 {
		_ = r.rdb.HDel(ctx, r.hashKey, stale...).Err()
	}
	return out, nil
}

// RequestDrain asks a node to begin a graceful drain. Delivery is best effort
// by design: a node that misses the message is either already gone (its
// pending entries will be reclaimed) or will be drained by SIGTERM.
func (r *Registry) RequestDrain(ctx context.Context, nodeID string) error {
	return r.rdb.Publish(ctx, r.drainChan, nodeID).Err()
}

// WatchDrain blocks until ctx is cancelled, invoking onDrain when a drain
// request names this node ("*" targets the whole cluster).
func (r *Registry) WatchDrain(ctx context.Context, nodeID string, onDrain func()) error {
	sub := r.rdb.Subscribe(ctx, r.drainChan)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if msg.Payload == nodeID || msg.Payload == "*" {
				onDrain()
			}
		}
	}
}
