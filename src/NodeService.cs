using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class NodeService : Node.NodeBase {
    int port;
    public NodeService(int port) {
        this.port = port;
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        return Task.FromResult(new ReadReply{Value = $"read val goes here, contacted port {port}"});
    }
}

}