using FASTER.darq;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class TransactionProcessorService : TransactionProcessor.TransactionProcessorBase {
    // long MinKey;
    private TransactionManager txnManager;
    private Table table;
    private Dictionary<(long, long), TransactionContext> externalTxnIdToTxnCtx = new Dictionary<(long, long), TransactionContext>();
    private IWriteAheadLog wal;
    public TransactionProcessorService(Table table, TransactionManager txnManager, IWriteAheadLog wal) {
        this.table = table;
        this.txnManager = txnManager;
        // MinKey = minKey;
        this.wal = wal;
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

    public override Task<EnqueueWorkloadReply> EnqueueWorkload(EnqueueWorkloadRequest request, ServerCallContext context)
    {
        // TODO: key should be ID of server 
        table.Write(new KeyAttr(1, 12345, table), new byte[]{1,2,3,4,5,6,7,8});

        var ctx = txnManager.Begin();
        Console.WriteLine("Should go to own");
        var own = table.Read(new TupleId(0, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        Console.WriteLine(own.ToString());
        foreach (var b in own.ToArray()){
            Console.WriteLine(b);
        }
        Console.WriteLine("Should RPC:");
        var other = table.Read(new TupleId(1, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        Console.WriteLine(other.ToString());
        foreach (var b in other.ToArray()){
            Console.WriteLine(b);
        }
        EnqueueWorkloadReply enqueueWorkloadReply = new EnqueueWorkloadReply{Success = true};
        return Task.FromResult(enqueueWorkloadReply);
    }

    public void Dispose(){
        table.Dispose();
        txnManager.Terminate();
    }
}

}