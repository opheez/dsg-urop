using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class TransactionProcessorService : TransactionProcessor.TransactionProcessorBase {
    long MinKey;
    public TransactionProcessorService(long minKey) {
        MinKey = minKey;
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        return Task.FromResult(new ReadReply{Value = $"read val goes here, contacted w minKey {MinKey}"});
    }
}

}