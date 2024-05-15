using FASTER.darq;
using FASTER.libdpr;
using System.Diagnostics;
using FASTER.common;
using System.Collections.Concurrent;


namespace DB 
{
public interface IWriteAheadLog
{
    public Task<long> RecordOk(long tid, long shard);
    public long Begin(long tid);
    public long Write(long tid, ref PrimaryKey pk, TupleDesc[] tupleDescs, byte[] val);
    
    public Task<long> Finish(long tid, LogType type);
    public Task<long> Prepare(Dictionary<long, List<(PrimaryKey, TupleDesc[], byte[])>> shardToWriteset, long tid);
    public Task<long> Finish2pc(long tid, LogType type, List<(long, long)> okLsnsToConsume);
    // public void SetCapabilities(IDarqProcessorClientCapabilities capabilities);
    public void Terminate();
    // public void Recover();

}

public class DarqWal : IWriteAheadLog {
    protected long currLsn = 0;
    protected Darq darq;
    protected IDarqProcessorClientCapabilities capabilities;
    protected DarqId partitionId;
    protected SimpleObjectPool<StepRequest> requestPool;
    internal ConcurrentDictionary<long, long> txnTbl = new ConcurrentDictionary<long, long>(); // ongoing transactions mapped to most recent lsn
    // requestBuilders should last for a Begin,Write,Finish cycle or TODO and should NEVER overlap 
    protected ConcurrentDictionary<long, StepRequestBuilder> requestBuilders = new ConcurrentDictionary<long, StepRequestBuilder>();
    protected ILogger logger;
    public DarqWal(DarqId partitionId, Darq darq, ILogger logger = null){
        this.partitionId = partitionId;
        this.darq = darq;
        this.logger = logger;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest());
    }

    public long Begin(long tid){
        // use GetOrAdd because may be in the middle of requestBuilder cycle
        StepRequestBuilder requestBuilder = requestBuilders.GetOrAdd(tid, _ => new StepRequestBuilder(requestPool.Checkout()));

        LogEntry entry = new LogEntry(GetNewLsn(), tid, LogType.Begin);
        entry.lsn = entry.prevLsn;
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        txnTbl[tid] = entry.lsn;

        return entry.lsn;
    }
     
    public long Write(long tid, ref PrimaryKey pk, TupleDesc[] tupleDescs, byte[] val){
        LogEntry entry = new LogEntry(txnTbl[tid], tid, pk, tupleDescs, val);
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid]; // throw error if doesn't exist
        entry.lsn = GetNewLsn();

        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        txnTbl[tid] = entry.lsn;
        return entry.lsn;
    }

    // TODO: add field for shard in log entry
    public async Task<long> RecordOk(long tid, long shard){
        // OK message is stepped by itself 
        StepRequestBuilder requestBuilder = new StepRequestBuilder(requestPool.Checkout());

        LogEntry entry = new LogEntry(txnTbl[tid], tid, LogType.Ok);
        entry.lsn = GetNewLsn();
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        txnTbl[tid] = entry.lsn;

        await StepAndReturnRequestBuilder(requestBuilder);
        return entry.lsn;
    }

    /// <summary>
    /// Commits or aborts a transaction
    /// </summary>
    /// <param name="entry"></param>
    /// <returns>lsn of finish log</returns>
    public async Task<long> Finish(long tid, LogType type){
        // on abort, requestBuilder may not have been created
        StepRequestBuilder requestBuilder = requestBuilders.GetOrAdd(tid, _ => new StepRequestBuilder(requestPool.Checkout()));

        long lsn = GetNewLsn();
        LogEntry entry = new LogEntry(txnTbl.GetValueOrDefault(tid, lsn), tid, type);
        entry.lsn = lsn;
        requestBuilder.AddRecoveryMessage(entry.ToBytes());

        await StepAndReturnRequestBuilder(requestBuilder);
        requestBuilders.Remove(entry.tid, out _);
        txnTbl.TryRemove(tid, out _);
        return entry.lsn;
    }

    /// <summary>
    /// Writes prepare log and sends out prepare messages to shards
    /// </summary>
    /// <param name="shardToWriteset"></param>
    /// <param name="tid"></param>
    /// <returns>lsn of prepare log</returns>
    public async Task<long> Prepare(Dictionary<long, List<(PrimaryKey, TupleDesc[], byte[])>> shardToWriteset, long tid) {
        StepRequestBuilder requestBuilder = requestBuilders[tid]; // throw error if doesn't exist

        // should be first 
        LogEntry entry = new LogEntry(GetNewLsn(), tid, LogType.Prepare);
        // TODO: make sure it is correct lsn/prevLsn values
        entry.lsn = entry.prevLsn;
        entry.pks = new PrimaryKey[0];
        entry.tupleDescs = new TupleDesc[0][];
        entry.vals = new byte[0][];
        // entry.pks = shardToWriteset.SelectMany(x => x.Value.Select(y => y.Item1)).ToArray();
        // entry.tupleDescs = shardToWriteset.SelectMany(x => x.Value.Select(y => y.Item2)).ToArray();
        // entry.vals = shardToWriteset.SelectMany(x => x.Value.Select(y => y.Item3)).ToArray();
        
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        txnTbl[tid] = entry.lsn;

        // add out message to each shard
        foreach (var shard in shardToWriteset) {
            long darqId = shard.Key;
            List<(PrimaryKey, TupleDesc[], byte[])> writeset = shard.Value;
            LogEntry outEntry = new LogEntry(partitionId.guid, tid, writeset.Select(x => x.Item1).ToArray(), writeset.Select(x => x.Item2).ToArray(), writeset.Select(x => x.Item3).ToArray());
            // PrintDebug($"sending prepare msg to {darqId} with keys {string.Join(", ", writeset.Select(x => x.Item1))}");
            requestBuilder.AddOutMessage(new DarqId(darqId), outEntry.ToBytes());
        }

        await StepAndReturnRequestBuilder(requestBuilder);
        requestBuilders.Remove(entry.tid, out _);
        return entry.lsn;
    }

    /// <summary>
    /// Commit or abort a transaction and consume OK messages from 2PC 
    /// </summary>
    /// <param name="tid"></param>
    /// <param name="type"></param>
    /// <param name="darqLsnsToConsume"></param>
    /// <returns>lsn of finish log</returns>
    public async Task<long> Finish2pc(long tid, LogType type, List<(long, long)> darqLsnsToConsume){
        StepRequestBuilder requestBuilder = new StepRequestBuilder(requestPool.Checkout());

        LogEntry entry = new LogEntry(txnTbl[tid], tid, type);
        entry.lsn = GetNewLsn();
        LogEntry outEntry = new LogEntry(0, entry.tid, LogType.Commit);
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        foreach (var item in darqLsnsToConsume) {
            (long darqLsn, long shard) = item;
            if (darqLsn == -1) continue;
            requestBuilder.MarkMessageConsumed(darqLsn);
            requestBuilder.AddOutMessage(new DarqId(shard), outEntry.ToBytes());
        }

        await StepAndReturnRequestBuilder(requestBuilder);
        requestBuilders.Remove(entry.tid, out _);
        txnTbl.TryRemove(tid, out _);
        return entry.lsn;
    }
    protected long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }

    protected async Task StepAndReturnRequestBuilder(StepRequestBuilder requestBuilder){
        StepRequest stepRequest = requestBuilder.FinishStep();
        await capabilities.Step(stepRequest);
        var version = darq.Version();
        await darq.DprCommit(version);
        requestPool.Return(stepRequest);
    }

    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }
    public void Terminate(){
        return;
    }

    void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[WAL {partitionId} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }

}

}