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


namespace DB 
{
public interface IWriteAheadLog
{
    public long Log(LogEntry entry);
    // public void Recover();

}

public class DARQWal : IWriteAheadLog {

    private long currLsn = 0;
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private SimpleObjectPool<StepRequest> requestPool;

    public DARQWal(WorkerId me){
        this.me = me;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest(null));
    }

    public (long, StepRequestBuilder) Begin(long tid){
        long lsn = GetNewLsn();
        LogEntry entry = new LogEntry(lsn, tid, LogType.Begin);
        entry.lsn = lsn;
        var requestBuilder = new StepRequestBuilder(requestPool.Checkout(), me);
        requestBuilder.AddSelfMessage(entry.ToBytes());
        return (lsn, requestBuilder);
    }
     
    public long Write(LogEntry entry, StepRequestBuilder requestBuilder){
        entry.lsn = GetNewLsn();

        requestBuilder.AddSelfMessage(entry.ToBytes());
        return entry.lsn;
    }

    public long Log(LogEntry entry){
        entry.lsn = GetNewLsn();
        var requestBuilder = new StepRequestBuilder(requestPool.Checkout(), me);
        requestBuilder.AddSelfMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        return entry.lsn;
    }

    public long Commit(LogEntry entry, StepRequestBuilder requestBuilder){
        entry.lsn = GetNewLsn();
        requestBuilder.AddSelfMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        return entry.lsn;
    }

    private long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }

    public void SetCapabilities(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }
}
public class BatchDARQWal : IWriteAheadLog {

    private long currLsn = 0;
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private SimpleObjectPool<StepRequest> requestPool;
    private BlockingCollection<LogEntry> buffer = new BlockingCollection<LogEntry>();
    private Thread logger;
    private int flushSize = 1000;

    public BatchDARQWal(WorkerId me){
        this.me = me;
        requestPool = new SimpleObjectPool<StepRequest>(() => new StepRequest(null));
        // start thread that flushes buffer every 1000 entries
        logger = new Thread(() => {
            while (true){
                var requestBuilder = new StepRequestBuilder(requestPool.Checkout(), me);
                var count = 0;
                foreach (LogEntry e in buffer.GetConsumingEnumerable()){
                    e.SetPersisted();
                    requestBuilder.AddSelfMessage(e.ToBytes());
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
        
        return (lsn, new StepRequestBuilder(requestPool.Checkout(), me));
    }
     
    public long Write(LogEntry entry, StepRequestBuilder requestBuilder){
        entry.lsn = GetNewLsn();

        AddToBuffer(entry);
        return entry.lsn;
    }

    public long Log(LogEntry entry){
        entry.lsn = GetNewLsn();
        var requestBuilder = new StepRequestBuilder(requestPool.Checkout(), me);
        requestBuilder.AddSelfMessage(entry.ToBytes());
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