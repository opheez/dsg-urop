using System;
using System.Text;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using System.Diagnostics;
using FASTER.common;

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
     
    public long Log(LogEntry entry){
        entry.lsn = GetNewLsn();
        if (entry.type == LogType.Begin){
            entry.prevLsn = entry.lsn;
        }

        var requestBuilder = new StepRequestBuilder(requestPool.Checkout(), me);
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

}