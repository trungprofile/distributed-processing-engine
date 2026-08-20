package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
)

// rpcTimeout bounds a single control-plane call. It is short on purpose: a
// coordinator that cannot answer inside this budget should surface as a
// Degraded condition and a requeue, not as a stalled reconcile.
const rpcTimeout = 3 * time.Second

// EngineClient is the operator's view of the coordinator's control plane.
//
// The operator deliberately reads cluster state through this interface rather
// than talking to Redis: XINFO GROUPS, the node registry and the drain
// broadcast already have a single owner, and duplicating that logic in the
// controller would give two components licence to disagree about how many
// records are outstanding.
type EngineClient interface {
	// ClusterStats reports consumer-group depth and per-node progress.
	ClusterStats(ctx context.Context, addr string) (*enginepb.ClusterStats, error)
	// DrainNode asks one node to stop reading and ack what it holds. It
	// returns once the request is broadcast, not once the node is drained.
	DrainNode(ctx context.Context, addr, nodeID string) error
	// Close releases every cached connection.
	Close() error
}

// grpcEngineClient dials coordinators lazily and keeps one connection per
// address. Reconciles run every few seconds against a small number of
// coordinators, so caching the connection matters more than reaping it.
type grpcEngineClient struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewEngineClient returns an EngineClient backed by plaintext gRPC. The
// coordinator listens inside the cluster network and exposes no mutating API
// beyond drain, so it is deployed without TLS; putting a mesh in front of it
// is a deployment decision, not an operator one.
func NewEngineClient() EngineClient {
	return &grpcEngineClient{conns: make(map[string]*grpc.ClientConn)}
}

func (c *grpcEngineClient) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial coordinator %s: %w", addr, err)
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *grpcEngineClient) ClusterStats(ctx context.Context, addr string) (*enginepb.ClusterStats, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	stats, err := enginepb.NewEngineClient(cc).GetClusterStats(ctx, &enginepb.GetClusterStatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("cluster stats from %s: %w", addr, err)
	}
	return stats, nil
}

func (c *grpcEngineClient) DrainNode(ctx context.Context, addr, nodeID string) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = enginepb.NewEngineClient(cc).DrainNode(ctx, &enginepb.DrainNodeRequest{NodeId: nodeID})
	if err != nil {
		return fmt.Errorf("drain %s via %s: %w", nodeID, addr, err)
	}
	return nil
}

func (c *grpcEngineClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, cc := range c.conns {
		_ = cc.Close()
		delete(c.conns, addr)
	}
	return nil
}
