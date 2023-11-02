using System;
using System.Text;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using System.Diagnostics;

namespace DB {

public delegate long LogWAL(LogEntry entry);
// Takes in requests for stored procedures and executes them
public class TransactionProcessor : IDarqProcessor {
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private List<WorkerId> workers;
    private StepRequest reusableRequest = new(null);
    private long currLsn = 0;
    
    public TransactionProcessor(WorkerId me, IDarqClusterInfo clusterInfo){
        this.me = me;
        workers = clusterInfo.GetWorkers().Select(e  => e.Item1).ToList();
    }

    public bool ProcessMessage(DarqMessage m){
        switch (m.GetMessageType()){
            case DarqMessageType.IN:
            {
                var storedProcedureName = Encoding.ASCII.GetString(m.GetMessageBody().ToArray());
                var requestBuilder = new StepRequestBuilder(reusableRequest, me);
                var schema = new (long, int)[]{};
                switch (storedProcedureName){
                    case "workloadA":
                        StoredProcedure p = StoredProcedure.GetWorkloadAUpdateHeavy(LogWAL);
                        p.Run();
                        break;
                    default:
                        throw new NotImplementedException();
                }
                // execute stored procedure
                // listen to transaction, if state is validated, add writeset to log? 
                // requestBuilder.AddSelfMessage(BitConverter.GetBytes(0));

                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.SELF: // this is on recovery; TODO: do we need to double pass?
                // count = BitConverter.ToInt32(m.GetMessageBody());
                Console.WriteLine($"Worker {me.guid} received SELF message: {LogEntry.FromBytes(m.GetMessageBody().ToArray())}");
                m.Dispose();
                return true;
            default:
                throw new NotImplementedException();
        }
    }

    public long LogWAL(LogEntry entry){
        entry.lsn = GetNewLsn();
        if (entry.type == LogType.Begin){
            entry.prevLsn = entry.lsn;
        }
        
        var requestBuilder = new StepRequestBuilder(reusableRequest, me);
        requestBuilder.AddSelfMessage(entry.ToBytes());
        var v = capabilities.Step(requestBuilder.FinishStep());
        Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
        return entry.lsn;
    }
    public void OnRestart(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    private long GetNewLsn() {
        return Interlocked.Increment(ref currLsn);
    }
}
}