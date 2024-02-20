using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class RpcService : TransactionProcessor.TransactionProcessorBase {
    long MinKey;
    private TransactionManager txnManager;
    private Table table;
    private Dictionary<(long, long), TransactionContext> externalTxnIdToTxnCtx = new Dictionary<(long, long), TransactionContext>();
    public RpcService(Table table, TransactionManager txnManager, long minKey) {
        this.table = table;
        this.txnManager = txnManager;
        MinKey = minKey;
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Reading from rpc service");
        (long, long) key = (request.Me, request.Tid);
        if (!externalTxnIdToTxnCtx.ContainsKey(key))
        {
            externalTxnIdToTxnCtx[key] = txnManager.Begin();
        }
        TransactionContext ctx = externalTxnIdToTxnCtx[key];
        TupleId tupleId = new TupleId(request.Key, table);
        TupleDesc[] tupleDescs = table.GetSchema();
        ReadReply reply = new ReadReply{ Value = ByteString.CopyFrom(table.Read(tupleId, tupleDescs, ctx))};
        return Task.FromResult(reply);
    }
}

}