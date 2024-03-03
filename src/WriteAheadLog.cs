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


namespace DB 
{
public interface IWriteAheadLog
{
    public long Log(LogEntry entry);
    public long Begin(long tid);
    public long Write(LogEntry entry);
    
    public long Commit(LogEntry entry);
    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities);
    public void Terminate();
    // public void Recover();

}

public class DARQWal : IWriteAheadLog {

    private long currLsn = 0;
    private IDarqProcessorClientCapabilities capabilities;
    private DarqId me;
    private SimpleObjectPool<StepRequest> requestPool;
    private ConcurrentDictionary<long, StepRequestBuilder> requestBuilders = new ConcurrentDictionary<long, StepRequestBuilder>();
    public DARQWal(DarqId me){
        this.me = me;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest());
    }

    public long Begin(long tid){
        long lsn = GetNewLsn();
        LogEntry entry = new LogEntry(lsn, tid, LogType.Begin);
        entry.lsn = lsn;
        var requestBuilder = requestBuilders.GetOrAdd(tid, _ => new StepRequestBuilder(requestPool.Checkout()));
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        return lsn;
    }
     
    public long Write(LogEntry entry){
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid];
        entry.lsn = GetNewLsn();

        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        return entry.lsn;
    }

    public long Log(LogEntry entry){
        entry.lsn = GetNewLsn();
        var requestBuilder = requestBuilders.GetOrAdd(entry.tid, _ => new StepRequestBuilder(requestPool.Checkout()));
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        requestBuilders.Remove(entry.tid, out _);
        return entry.lsn;
    }

    public void Send(DarqId recipient, LogEntry entry){
        var requestBuilder = new StepRequestBuilder(requestPool.Checkout());
        requestBuilder.AddOutMessage(recipient, entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
    }

    /// <summary>
    /// Commits or aborts a transaction
    /// </summary>
    /// <param name="entry"></param>
    /// <returns></returns>
    public long Commit(LogEntry entry){
        entry.lsn = GetNewLsn();
        StepRequestBuilder requestBuilder = requestBuilders[entry.tid];
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        requestBuilders.Remove(entry.tid, out _);
        return entry.lsn;
    }

    private long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }

    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }
    public void Terminate(){
        return;
    }

}
// deprecated 
public class BatchDARQWal {

    private long currLsn = 0;
    private IDarqProcessorClientCapabilities capabilities;
    private DarqId me;
    private SimpleObjectPool<StepRequest> requestPool;
    private BlockingCollection<LogEntry> buffer = new BlockingCollection<LogEntry>();
    private Thread logger;
    private int flushSize = 1000;

    public BatchDARQWal(DarqId me){
        this.me = me;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest());
        // start thread that flushes buffer every 1000 entries
        logger = new Thread(() => {
            while (true){
                var requestBuilder = new StepRequestBuilder(requestPool.Checkout());
                var count = 0;
                foreach (LogEntry e in buffer.GetConsumingEnumerable()){
                    e.SetPersisted();
                    requestBuilder.AddRecoveryMessage(e.ToBytes());
                    count++;
                    if (count >= flushSize) break;
                }
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                
                Console.WriteLine("flushed");
            }
        });
        logger.Start();
    }

    private void AddToBuffer(LogEntry entry){
        buffer.Add(entry);
        while (entry.persited == false){
            Thread.Yield();
        }
    }

    public (long, StepRequestBuilder) Begin(long tid){
        long lsn = GetNewLsn();
        LogEntry entry = new LogEntry(lsn, tid, LogType.Begin);
        entry.lsn = lsn;
        AddToBuffer(entry);
        
        return (lsn, new StepRequestBuilder(requestPool.Checkout()));
    }
     
    public long Write(LogEntry entry, StepRequestBuilder requestBuilder){
        entry.lsn = GetNewLsn();

        AddToBuffer(entry);
        return entry.lsn;
    }

    public long Log(LogEntry entry){
        entry.lsn = GetNewLsn();
        var requestBuilder = new StepRequestBuilder(requestPool.Checkout());
        requestBuilder.AddRecoveryMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        return entry.lsn;
    }

    public long Commit(LogEntry entry, StepRequestBuilder requestBuilder){
        entry.lsn = GetNewLsn();
        AddToBuffer(entry);
        return entry.lsn;
    }

    private long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }

    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    public void Terminate(){
        logger.Interrupt();
    }
}

}