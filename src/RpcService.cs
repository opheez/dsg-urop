using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class RpcService : TransactionProcessor.TransactionProcessorBase {
    long MinKey;
    private TransactionManager txnManager;
    private Table table;
    public RpcService(Table table, TransactionManager txnManager, long minKey) {
        this.table = table;
        this.txnManager = txnManager;
        MinKey = minKey;
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Reading from rpc service");
        ReadReply reply = new ReadReply{ Value = ByteString.CopyFrom(table.Read(new TupleId(request.Key, table)))};
        return Task.FromResult(reply);
    }
}

}