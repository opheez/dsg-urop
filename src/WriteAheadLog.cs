using System;
using System.Text;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using System.Diagnostics;
using FASTER.common;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Grpc.Net.Client;
using darq;
using System.Transactions;


namespace DB 
{
public interface IWriteAheadLog
{
    public long Log(LogEntry entry);
    public long Begin(long tid);
    public long Write(LogEntry entry);
    
    public long Finish(LogEntry entry);
    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities);
    public void Terminate();
    // public void Recover();

}

public class DARQWal : IWriteAheadLog {

    protected long currLsn = 0;
    protected IDarqProcessorClientCapabilities capabilities;
    protected DarqId me;
    protected SimpleObjectPool<StepRequest> requestPool;
    // requestBuilders should last for a Begin,Write,Finish cycle or TODO and should NEVER overlap 
    protected ConcurrentDictionary<long, StepRequestBuilder> requestBuilders = new ConcurrentDictionary<long, StepRequestBuilder>();
    public DARQWal(DarqId me){
        this.me = me;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest());
    }

    public long Begin(long tid){
        // use GetOrAdd because may be in the middle of requestBuilder cycle
        StepRequestBuilder requestBuilder = requestBuilders.GetOrAdd(tid, _ => new StepRequestBuilder(requestPool.Checkout()));

        long lsn = GetNewLsn();
        LogEntry entry = new LogEntry(lsn, tid, LogType.Begin);
        entry.lsn = lsn;
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        return lsn;
    }
     
    public long Write(LogEntry entry){
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid]; // throw error if doesn't exist
        entry.lsn = GetNewLsn();

        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        return entry.lsn;
    }

    public long Log(LogEntry entry){
        StepRequestBuilder requestBuilder = new StepRequestBuilder(requestPool.Checkout());

        entry.lsn = GetNewLsn();
        requestBuilder.AddRecoveryMessage(entry.ToBytes());

        StepAndReturnRequestBuilder(requestBuilder);
        return entry.lsn;
    }

    /// <summary>
    /// Commits or aborts a transaction
    /// </summary>
    /// <param name="entry"></param>
    /// <returns></returns>
    virtual public long Finish(LogEntry entry){
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid]; // throw error if doesn't exist

        entry.lsn = GetNewLsn();
        requestBuilder.AddRecoveryMessage(entry.ToBytes());

        StepAndReturnRequestBuilder(requestBuilder);
        requestBuilders.Remove(entry.tid, out _);
        return entry.lsn;
    }

    protected long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }

    protected void StepAndReturnRequestBuilder(StepRequestBuilder requestBuilder){
        StepRequest stepRequest = requestBuilder.FinishStep();
        var v = capabilities.Step(stepRequest);
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        requestPool.Return(stepRequest);
    }

    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }
    public void Terminate(){
        return;
    }

}

public class ShardedDarqWal : DARQWal {
    public ShardedDarqWal(DarqId me) : base(me) {
    }

    public long Prepare(Dictionary<long, List<(KeyAttr, byte[])>> shardToWriteset, LogEntry entry) {
        StepRequestBuilder requestBuilder = new StepRequestBuilder(requestPool.Checkout());

        // TODO: make sure it is correct lsn/prevLsn values
        entry.lsn = GetNewLsn();
        entry.keyAttrs = shardToWriteset.SelectMany(x => x.Value.Select(y => y.Item1)).ToArray();
        entry.vals = shardToWriteset.SelectMany(x => x.Value.Select(y => y.Item2)).ToArray();
        
        requestBuilder.AddRecoveryMessage(entry.ToBytes());

        // add out message to each shard
        foreach (var shard in shardToWriteset) {
            long darqId = shard.Key;
            List<(KeyAttr, byte[])> writeset = shard.Value;
            LogEntry outEntry = new LogEntry(0, entry.tid, writeset.Select(x => x.Item1).ToArray(),  writeset.Select(x => x.Item2).ToArray());
            Console.WriteLine($"sending prepare msg to {darqId} with keys {string.Join(", ", writeset.Select(x => x.Item1))}");
            requestBuilder.AddOutMessage(new DarqId(darqId), outEntry.ToBytes());
        }

        StepAndReturnRequestBuilder(requestBuilder);
        return entry.lsn;
    }

    public long Finish(LogEntry entry, List<long> darqLsnsToConsume){
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid]; // throw error if doesn't exist

        entry.lsn = GetNewLsn();
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        foreach (var darqLsn in darqLsnsToConsume) {
            requestBuilder.MarkMessageConsumed(darqLsn);
        }

        StepAndReturnRequestBuilder(requestBuilder);
        requestBuilders.Remove(entry.tid, out _);
        return entry.lsn;
    }
}

// deprecated, made for 6.810 report
// public class BatchDARQWal {

//     private long currLsn = 0;
//     private IDarqProcessorClientCapabilities capabilities;
//     private DarqId me;
//     private SimpleObjectPool<StepRequest> requestPool;
//     private BlockingCollection<LogEntry> buffer = new BlockingCollection<LogEntry>();
//     private Thread logger;
//     private int flushSize = 1000;

//     public BatchDARQWal(DarqId me){
//         this.me = me;
//         requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest());
//         // start thread that flushes buffer every 1000 entries
//         logger = new Thread(() => {
//             while (true){
//                 var requestBuilder = new StepRequestBuilder(requestPool.Checkout());
//                 var count = 0;
//                 foreach (LogEntry e in buffer.GetConsumingEnumerable()){
//                     e.SetPersisted();
//                     requestBuilder.AddRecoveryMessage(e.ToBytes());
//                     count++;
//                     if (count >= flushSize) break;
//                 }
//                 var v = capabilities.Step(requestBuilder.FinishStep());
//                 Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                
//                 Console.WriteLine("flushed");
//             }
//         });
//         logger.Start();
//     }

//     private void AddToBuffer(LogEntry entry){
//         buffer.Add(entry);
//         while (entry.persited == false){
//             Thread.Yield();
//         }
//     }

//     public (long, StepRequestBuilder) Begin(long tid){
//         long lsn = GetNewLsn();
//         LogEntry entry = new LogEntry(lsn, tid, LogType.Begin);
//         entry.lsn = lsn;
//         AddToBuffer(entry);
        
//         return (lsn, new StepRequestBuilder(requestPool.Checkout()));
//     }
     
//     public long Write(LogEntry entry, StepRequestBuilder requestBuilder){
//         entry.lsn = GetNewLsn();

//         AddToBuffer(entry);
//         return entry.lsn;
//     }

//     public long Log(LogEntry entry){
//         entry.lsn = GetNewLsn();
//         var requestBuilder = new StepRequestBuilder(requestPool.Checkout());
//         requestBuilder.AddRecoveryMessage(entry.ToBytes());
//         var v = capabilities.Step(requestBuilder.FinishStep());
//         Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
//         return entry.lsn;
//     }

//     public long Commit(LogEntry entry, StepRequestBuilder requestBuilder){
//         entry.lsn = GetNewLsn();
//         AddToBuffer(entry);
//         return entry.lsn;
//     }

//     private long GetNewLsn() {
//         return Interlocked.Increment(ref currLsn);
//     }

//     public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
//         this.capabilities = capabilities;
//     }

//     public void Terminate(){
//         logger.Interrupt();
//     }
// }

}