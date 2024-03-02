using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;

namespace DB {

public class TransactionProcessorService : TransactionProcessor.TransactionProcessorBase {
    // long MinKey;
    private TransactionManager txnManager;
    private Table table;
    private Dictionary<(long, long), TransactionContext> externalTxnIdToTxnCtx = new Dictionary<(long, long), TransactionContext>();
    private IDarqWal wal;
    private long me;
    private ThreadLocalObjectPool<byte[]> enqueueRequestPool;
    public TransactionProcessorService(long me, Table table, TransactionManager txnManager, IDarqWal wal) {
        this.table = table;
        this.txnManager = txnManager;
        // MinKey = minKey;
        this.me = me;
        this.wal = wal;
        table.Write(new KeyAttr(me, 12345, table), new byte[]{1,2,3,4,5,6,7,8});
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

    // typically used for Prepare() and Commit() 
    public override Task<WalReply> WriteWalEntry(WalRequest request, ServerCallContext context)
    {
        Console.WriteLine("Writing to WAL");

        var enqueueBuffer = enqueueRequestPool.Checkout();
        SerializedDarqEntryBatch enqueueRequest;
        unsafe
        {
            fixed (byte* b = enqueueBuffer)
            {
                enqueueRequest = new SerializedDarqEntryBatch(b);
                enqueueRequest.SetContent(request.Message.Span);
            }
        }

        var ok = wal.GetDarqProcessor().GetBackend().Enqueue(enqueueRequest, request.ProducerId, request.Lsn);
        enqueueRequestPool.Return(enqueueBuffer);
        return Task.FromResult(new DarqEnqueueResult
        {
            Ok = ok
        });
    }

    public void Dispose(){
        table.Dispose();
        txnManager.Terminate();
    }
}

}