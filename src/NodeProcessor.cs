using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {
public class NodeProcessor : Node.NodeBase {
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        return Task.FromResult(new ReadReply{Value = "read val goes here"});
    }
}

}