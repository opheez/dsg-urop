using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using FASTER.client;
using FASTER.core;
using System.Diagnostics;
using Grpc.Net.Client;
using darq;
using darq.client;
using System.Collections.Concurrent;

namespace DB {

public class DarqTransactionProcessorService : TransactionProcessor.TransactionProcessorBase, IDarqProcessor {
    private ShardedTransactionManager txnManager;
    private ShardedTable table;
    private Dictionary<(long, long), long> externalToInternalTxnId = new Dictionary<(long, long), long>();
    private Dictionary<long, TransactionContext> txnIdToTxnCtx = new Dictionary<long, TransactionContext>();
    private DarqWal wal;
    private long me;
    // from darqProcessor
    private Darq backend;
    private readonly DarqBackgroundTask _backgroundTask;
    private readonly DarqBackgroundWorkerPool workerPool;
    private readonly ManualResetEventSlim terminationStart, terminationComplete;
    private Thread refreshThread, processingThread;
    private ColocatedDarqProcessorClient<RwLatchVersionScheme> processorClient;

    private IDarqProcessorClientCapabilities capabilities;

    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private int nextWorker = 0;

    /// previously
    // private IDarqProcessorClientCapabilities capabilities;
    // private DarqId me;
    private StepRequest reusableRequest = new();

    // TODO: condense table into tables
    Dictionary<int, Table> tables = new Dictionary<int, Table>();
    Dictionary<DarqId, GrpcChannel> clusterMap;
    public DarqTransactionProcessorService(long me, ShardedTable table, ShardedTransactionManager txnManager, DarqWal wal, Darq darq, DarqBackgroundWorkerPool workerPool, Dictionary<DarqId, GrpcChannel> clusterMap) {
        this.table = table;
        this.txnManager = txnManager;
        this.me = me;
        this.wal = wal;
        this.clusterMap = clusterMap;
        table.Write(new KeyAttr(me, 12345, table), new byte[]{1,2,3,4,5,6,7,8});

        backend = darq;
        _backgroundTask = new DarqBackgroundTask(backend, workerPool, session => new TransactionProcessorProducerWrapper(clusterMap, session));
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new ManualResetEventSlim();
        this.workerPool = workerPool;
        backend.ConnectToCluster();
        
        _backgroundTask.BeginProcessing();

        refreshThread = new Thread(() =>
        {
            while (!terminationStart.IsSet)
                backend.Refresh();
            terminationComplete.Set();
        });
        refreshThread.Start();

        processorClient = new ColocatedDarqProcessorClient<RwLatchVersionScheme>(backend);
        processingThread = new Thread(() =>
        {
            processorClient.StartProcessing(this);
        });
        processingThread.Start();
        // TODO(Tianyu): Hacky
        // spin until we are sure that we have started 
        while (capabilities == null) {}
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Reading from rpc service");
        long internalTid = GetOrRegisterTid(request.Me, request.Tid);
        TransactionContext ctx = txnIdToTxnCtx[internalTid];
        TupleId tupleId = new TupleId(request.Key, table);
        TupleDesc[] tupleDescs = table.GetSchema();
        ReadReply reply = new ReadReply{ Value = ByteString.CopyFrom(table.Read(tupleId, tupleDescs, ctx))};
        return Task.FromResult(reply);
    }

    public override Task<EnqueueWorkloadReply> EnqueueWorkload(EnqueueWorkloadRequest request, ServerCallContext context)
    {
        BenchmarkConfig testCfg = new BenchmarkConfig(
            ratio: 0.2,
            attrCount: 10,
            threadCount: 2,
            iterationCount: 1,
            perThreadDataCount: 2
        );
        TableBenchmark b = new ShardedBenchmark("2pc", testCfg, txnManager, table, wal);
        b.RunTransactions();

        // txnManager.Run();
        // var ctx = txnManager.Begin();
        // Console.WriteLine("Should go to own");
        // var own = table.Read(new TupleId(0, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        // Console.WriteLine(own.ToString());
        // foreach (var b in own.ToArray()){
        //     Console.WriteLine(b);
        // }
        // Console.WriteLine("Should RPC:");
        // var other = table.Read(new TupleId(1, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        // Console.WriteLine(other.ToString());
        // foreach (var b in other.ToArray()){
        //     Console.WriteLine(b);
        // }
        // Console.WriteLine("Starting commit");
        // txnManager.Commit(ctx);
        // txnManager.Terminate();
        EnqueueWorkloadReply enqueueWorkloadReply = new EnqueueWorkloadReply{Success = true};
        return Task.FromResult(enqueueWorkloadReply);
    }

    // typically used for Prepare() and Commit() 
    public override async Task<WalReply> WriteWalEntry(WalRequest request, ServerCallContext context)
    {
        Console.WriteLine($"Writing to WAL from {request.Me}");
        LogEntry entry = LogEntry.FromBytes(request.Message.ToArray(), tables);

        if (entry.type == LogType.Prepare || entry.type == LogType.Commit)
        {
            long internalTid = GetOrRegisterTid(request.Me, request.Tid);
            entry.lsn = internalTid; // TODO: HACKY reuse, we keep tid to be original tid
            entry.prevLsn = request.Me; // TODO: hacky place to put sender id
        }
        
        var stepRequest = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(stepRequest);
        // TODO: do we need to step messages consumed, self, and out messages 
        requestBuilder.AddSelfMessage(entry.ToBytes());
        await capabilities.Step(requestBuilder.FinishStep());
        Console.WriteLine($"Workflow {entry.lsn} started");
        stepRequestPool.Return(stepRequest);
        backend.EndAction();
        return new WalReply{Success = true};
    }

    private long GetOrRegisterTid(long me, long tid) {
        Console.WriteLine($"Getting or registering tid: ({me}, {tid})");
        if (externalToInternalTxnId.ContainsKey((me, tid))) 
            return externalToInternalTxnId[(me, tid)];

        var ctx = txnManager.Begin();
        long internalTid = ctx.tid;
        Console.WriteLine("Registering new tid: " + internalTid);
        externalToInternalTxnId[(me, tid)] = internalTid;
        txnIdToTxnCtx[internalTid] = ctx;
        return internalTid;
    }

    public void Dispose(){
        table.Dispose();
        txnManager.Terminate();
        terminationStart.Set();
        // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
        backend.ForceCheckpoint();
        Thread.Sleep(1000);
        _backgroundTask.StopProcessing();
        _backgroundTask.Dispose();
        processorClient.StopProcessingAsync().GetAwaiter().GetResult();
        processorClient.Dispose();
        terminationComplete.Wait();
        refreshThread.Join();
        processingThread.Join();
    }

    public Darq GetBackend() => backend;

    public bool ProcessMessage(DarqMessage m){
        Console.WriteLine($"Processing message");
        bool recoveryMode = false;
        switch (m.GetMessageType()){
            case DarqMessageType.IN:
            {
                unsafe
                {
                    fixed (byte* b = m.GetMessageBody())
                    {
                        int signal = *(int*)b;
                        // This is a special termination signal
                        if (signal == -1)
                        {
                            m.Dispose();
                            // Return false to signal that there are no more messages to process and the processing
                            // loop can exit
                            return false;
                        }
                    }
                }

                LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray(), tables);
                var requestBuilder = new StepRequestBuilder(reusableRequest);
                // requestBuilder.AddRecoveryMessage(m.GetMessageBody());
                switch (entry.type)
                {
                    // Coordinator side
                    case LogType.Ok:
                    {
                        Console.WriteLine($"Got OK log entry: {entry}");
                        txnManager.MarkAcked(entry.tid, TransactionStatus.Validated, m.GetLsn(), entry.prevLsn);
                        m.Dispose();
                        return true;
                    }
                    case LogType.Ack:
                    {
                        Console.WriteLine($"Got ACK log entry: {entry}");
                        // can ignore in DARQ since we know out commit message is sent
                        m.Dispose();
                        return true;
                    }
                    // Worker side
                    case LogType.Prepare:
                    {
                        Console.WriteLine($"Got prepare log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""

                        // add each write to context before validating
                        TransactionContext ctx = txnIdToTxnCtx[internalTid];
                        for (int i = 0; i < entry.keyAttrs.Length; i++)
                        {
                            KeyAttr keyAttr = entry.keyAttrs[i];
                            (int, int) metadata = table.GetAttrMetadata(keyAttr.Attr);
                            ctx.AddWriteSet(new TupleId(keyAttr.Key, table), new TupleDesc[]{new TupleDesc(keyAttr.Attr, metadata.Item1, metadata.Item2)}, entry.vals[i]);
                        }
                        bool success = txnManager.Validate(ctx);
                        Console.WriteLine($"Validated at node {me}: {success}; now sending OK to {sender}");
                        if (success) {
                            LogEntry okEntry = new LogEntry(me, entry.tid, LogType.Ok);
                            requestBuilder.AddOutMessage(new DarqId(sender), okEntry.ToBytes());
                        }
                        break;
                    }
                    case LogType.Commit:
                    {
                        Console.WriteLine($"Got commit log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""
                        
                        txnManager.Write(txnIdToTxnCtx[internalTid], (tid, type) => wal.Finish(tid, type));

                        Console.WriteLine($"Committed at node {me}; now sending ACK to {sender}");
                        LogEntry ackEntry = new LogEntry(me, entry.tid, LogType.Ack);
                        requestBuilder.AddOutMessage(new DarqId(sender), ackEntry.ToBytes());
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }

                
                m.Dispose();
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.RECOVERY: // this is on recovery; TODO: do we need to double pass?
                Console.WriteLine($"Recovering?, got log");
                if (recoveryMode) {
                    LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray(), tables);
                    
                    Console.WriteLine($"Recovering, got log entry: {entry}");

                }
                m.Dispose();
                return true;
            default:
                throw new NotImplementedException();
        }
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
        this.wal.SetCapabilities(capabilities);
    }
}


public class TransactionProcessorProducerWrapper : IDarqProducer
{
    private Dictionary<DarqId, GrpcChannel> clusterMap;
    private ConcurrentDictionary<DarqId, TransactionProcessor.TransactionProcessorClient> clients = new();
    private DprSession session;

    public TransactionProcessorProducerWrapper(Dictionary<DarqId, GrpcChannel> clusterMap, DprSession session)
    {
        this.clusterMap = clusterMap;
        this.session = session;
    }
    
    public void Dispose() {}

    public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback, long producerId, long lsn)
    {
        LogEntry entry = LogEntry.FromBytes(message.ToArray(), new Dictionary<int, Table>());
        Console.WriteLine("am making an out request");
        var client = clients.GetOrAdd(darqId,
            _ => new TransactionProcessor.TransactionProcessorClient(clusterMap[darqId]));
        var walRequest = new WalRequest
        {
            Message = ByteString.CopyFrom(message),
            Tid = entry.tid,
            Me = producerId,
            Lsn = lsn,
        };
        Task.Run(async () =>
        {
            try
            {
                await client.WriteWalEntryAsync(walRequest);
                callback(true);
            }
            catch
            {
                callback(false);
                throw;
            }
        });
    }

    public void ForceFlush()
    {
        // TODO(Tianyu): Not implemented for now
    }
}
}