// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             (unknown)
// source: sharddistributor/v1/distributor.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ShardDistributorService_ShardDistributorStream_FullMethodName = "/sharddistributor.v1.ShardDistributorService/ShardDistributorStream"
)

// ShardDistributorServiceClient is the client API for ShardDistributorService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ShardDistributorService service provides fast shard distribution notifications
type ShardDistributorServiceClient interface {
	// ShardDistributorStream provides a bidirectional stream for all communications:
	// - Instance registration and deregistration
	// - Shard assignment watching and heartbeats
	// - Status updates
	ShardDistributorStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ShardDistributorStreamRequest, ShardDistributorStreamResponse], error)
}

type shardDistributorServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewShardDistributorServiceClient(cc grpc.ClientConnInterface) ShardDistributorServiceClient {
	return &shardDistributorServiceClient{cc}
}

func (c *shardDistributorServiceClient) ShardDistributorStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[ShardDistributorStreamRequest, ShardDistributorStreamResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &ShardDistributorService_ServiceDesc.Streams[0], ShardDistributorService_ShardDistributorStream_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ShardDistributorStreamRequest, ShardDistributorStreamResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ShardDistributorService_ShardDistributorStreamClient = grpc.BidiStreamingClient[ShardDistributorStreamRequest, ShardDistributorStreamResponse]

// ShardDistributorServiceServer is the server API for ShardDistributorService service.
// All implementations must embed UnimplementedShardDistributorServiceServer
// for forward compatibility.
//
// ShardDistributorService service provides fast shard distribution notifications
type ShardDistributorServiceServer interface {
	// ShardDistributorStream provides a bidirectional stream for all communications:
	// - Instance registration and deregistration
	// - Shard assignment watching and heartbeats
	// - Status updates
	ShardDistributorStream(grpc.BidiStreamingServer[ShardDistributorStreamRequest, ShardDistributorStreamResponse]) error
	mustEmbedUnimplementedShardDistributorServiceServer()
}

// UnimplementedShardDistributorServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedShardDistributorServiceServer struct{}

func (UnimplementedShardDistributorServiceServer) ShardDistributorStream(grpc.BidiStreamingServer[ShardDistributorStreamRequest, ShardDistributorStreamResponse]) error {
	return status.Errorf(codes.Unimplemented, "method ShardDistributorStream not implemented")
}
func (UnimplementedShardDistributorServiceServer) mustEmbedUnimplementedShardDistributorServiceServer() {
}
func (UnimplementedShardDistributorServiceServer) testEmbeddedByValue() {}

// UnsafeShardDistributorServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ShardDistributorServiceServer will
// result in compilation errors.
type UnsafeShardDistributorServiceServer interface {
	mustEmbedUnimplementedShardDistributorServiceServer()
}

func RegisterShardDistributorServiceServer(s grpc.ServiceRegistrar, srv ShardDistributorServiceServer) {
	// If the following call pancis, it indicates UnimplementedShardDistributorServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ShardDistributorService_ServiceDesc, srv)
}

func _ShardDistributorService_ShardDistributorStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ShardDistributorServiceServer).ShardDistributorStream(&grpc.GenericServerStream[ShardDistributorStreamRequest, ShardDistributorStreamResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ShardDistributorService_ShardDistributorStreamServer = grpc.BidiStreamingServer[ShardDistributorStreamRequest, ShardDistributorStreamResponse]

// ShardDistributorService_ServiceDesc is the grpc.ServiceDesc for ShardDistributorService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ShardDistributorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "sharddistributor.v1.ShardDistributorService",
	HandlerType: (*ShardDistributorServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ShardDistributorStream",
			Handler:       _ShardDistributorService_ShardDistributorStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "sharddistributor/v1/distributor.proto",
}
