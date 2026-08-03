// Package grpcserver implements the Engine control-plane service. It is
// deliberately thin: submissions go straight to the stream, stats are read
// from the consumer group and the node registry, and drain is a signal rather
// than an orchestration.
package grpcserver

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	"github.com/trungprofile/distributed-processing-engine/internal/cluster"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// maxBatch bounds one SubmitBatch call so a single RPC cannot pin an
// unbounded pipeline in Redis.
const maxBatch = 10_000

// Server implements enginepb.EngineServer.
type Server struct {
	enginepb.UnimplementedEngineServer

	pub *stream.Consumer
	reg *cluster.Registry
}

// New wires the service to a stream publisher and the node registry.
func New(pub *stream.Consumer, reg *cluster.Registry) *Server {
	return &Server{pub: pub, reg: reg}
}

// SubmitBatch appends records to the stream.
func (s *Server) SubmitBatch(ctx context.Context, req *enginepb.SubmitBatchRequest) (*enginepb.SubmitBatchResponse, error) {
	recs := req.GetRecords()
	if len(recs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "records must not be empty")
	}
	if len(recs) > maxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "batch of %d exceeds limit %d", len(recs), maxBatch)
	}

	out := make([]stream.Record, 0, len(recs))
	for i, r := range recs {
		if r.GetKey() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "records[%d]: key is required as the idempotency key", i)
		}
		out = append(out, stream.Record{Key: r.GetKey(), Payload: r.GetPayload()})
	}
	if err := s.pub.Publish(ctx, out...); err != nil {
		return nil, status.Errorf(codes.Unavailable, "publish: %v", err)
	}
	return &enginepb.SubmitBatchResponse{
		Accepted: int64(len(out)),
		Stream:   s.pub.Stream(),
	}, nil
}

// GetClusterStats merges per-node heartbeats with consumer-group depth.
func (s *Server) GetClusterStats(ctx context.Context, _ *enginepb.GetClusterStatsRequest) (*enginepb.ClusterStats, error) {
	nodes, err := s.reg.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "registry: %v", err)
	}
	depth, err := s.pub.Stats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "stream stats: %v", err)
	}

	resp := &enginepb.ClusterStats{
		StreamLag:    depth.Lag,
		Pending:      depth.Pending,
		StreamLength: depth.Length,
		Nodes:        make([]*enginepb.NodeStats, 0, len(nodes)),
	}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &enginepb.NodeStats{
			NodeId:            n.NodeID,
			Capacity:          n.Capacity,
			InFlight:          n.InFlight,
			Processed:         n.Processed,
			Reclaimed:         n.Reclaimed,
			Draining:          n.Draining,
			LastHeartbeatUnix: n.UpdatedAt,
		})
	}
	return resp, nil
}

// DrainNode publishes a drain request. The response confirms the request was
// broadcast, not that the node finished draining; poll GetClusterStats for
// that.
func (s *Server) DrainNode(ctx context.Context, req *enginepb.DrainNodeRequest) (*enginepb.DrainNodeResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required (use \"*\" for the whole cluster)")
	}
	if err := s.reg.RequestDrain(ctx, req.GetNodeId()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "drain %s: %v", req.GetNodeId(), err)
	}
	return &enginepb.DrainNodeResponse{Requested: true}, nil
}

// Serve runs the gRPC server until ctx is cancelled, then stops it gracefully
// so in-flight RPCs complete.
func (s *Server) Serve(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	enginepb.RegisterEngineServer(srv, s)

	hs := health.NewServer()
	hs.SetServingStatus("engine.v1.Engine", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		srv.GracefulStop()
		return ctx.Err()
	}
}
