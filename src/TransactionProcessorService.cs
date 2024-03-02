using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using System.Diagnostics;
using FASTER.common;
using Grpc.Net.Client;
using darq;
using darq.client;
using System.Collections.Concurrent;
using Google.Protobuf;


namespace DB {

public class WalHandle
{
    internal (long, long) externalTid;
    internal LogType type;
    internal byte[] result;
    internal TaskCompletionSource<WalReply> tcs = new();

    public WalHandle((long, long) externalTid, LogType type)
    {
        this.externalTid = externalTid;
        this.type = type;
    }
}

public class TransactionProcessorService : TransactionProcessor.TransactionProcessorBase, IDarqProcessor {
    private TransactionManager txnManager;
    private Table table;
    private Dictionary<(long, long), TransactionContext> externalTxnIdToTxnCtx = new Dictionary<(long, long), TransactionContext>();
    private IDarqWal wal;
    private long me;
    private ConcurrentDictionary<(long, long), WalHandle> startedWalRequests;

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
    Dictionary<int, Table> tables = new Dictionary<int, Table>();
    public TransactionProcessorService(long me, Table table, TransactionManager txnManager, IDarqWal wal, Darq darq, DarqBackgroundWorkerPool workerPool, Dictionary<DarqId, GrpcChannel> clusterMap) {
        this.table = table;
        this.txnManager = txnManager;
        // MinKey = minKey;
        this.me = me;
        this.wal = wal;
        table.Write(new KeyAttr(me, 12345, table), new byte[]{1,2,3,4,5,6,7,8});

        backend = darq;
        _backgroundTask = new DarqBackgroundTask(backend, workerPool, session => new TransactionProcessorProducerWrapper(clusterMap, session));
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new ManualResetEventSlim();
        this.workerPool = workerPool;
        backend.ConnectToCluster();
        
        _backgroundTask.StopProcessing();

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
    public override async Task<WalReply> WriteWalEntry(WalRequest request, ServerCallContext context)
    {
        Console.WriteLine("Writing to WAL");
        LogEntry entry = LogEntry.FromBytes(request.Message.ToArray(), tables);

        var handle = new WalHandle((request.Me, request.Tid), entry.type);
        var actualHandle = startedWalRequests.GetOrAdd((request.Me, request.Tid), handle);
        if (actualHandle == handle)
        {
            // This handle was created by this thread, which gives us the ability to go ahead and start the workflow
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.AddRecoveryMessage(request.Message.ToArray());
            requestBuilder.AddSelfMessage(request.Message.ToArray());
            await capabilities.Step(requestBuilder.FinishStep());
            Console.WriteLine($"Workflow {(request.Me, request.Tid)} started");
            stepRequestPool.Return(stepRequest);
        }
        backend.EndAction();
        return await GetWalRequestResultAsync(handle);
    }

    private async Task<WalReply> GetWalRequestResultAsync(WalHandle handle)
    {
        while (true)
        {
            var s = backend.DetachFromWorker();
            var result = await handle.tcs.Task;
            if (backend.TryMergeAndStartAction(s)) return result;
            // Otherwise, there has been a rollback, should retry with a new handle, if any
            while (!startedWalRequests.TryGetValue(handle.externalTid, out handle))
                await Task.Yield();                
        }
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
                switch (entry.type)
                {
                    case LogType.Prepare:
                        Console.WriteLine($"Got prepare log entry: {entry}");
                        break;
                    case LogType.Commit:
                        Console.WriteLine($"Got commit log entry: {entry}");
                        // write self message
                        var workflowHandle = startedWalRequests[(entry.tid, entry.tid)];
                        break;
                    default:
                        throw new NotImplementedException();
                }

                var requestBuilder = new StepRequestBuilder(reusableRequest);
                requestBuilder.MarkMessageConsumed(m.GetLsn());
                requestBuilder.AddRecoveryMessage(m.GetMessageBody());
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