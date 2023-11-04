using System;
using System.Text;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using System.Diagnostics;

namespace DB {

// Takes in requests for stored procedures and executes them
public class DarqTransactionProcessor : IDarqProcessor {
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private List<WorkerId> workers;
    private StepRequest reusableRequest = new(null);
    private DARQWal wal;
    
    public DarqTransactionProcessor(WorkerId me, IDarqClusterInfo clusterInfo){
        this.me = me;
        workers = clusterInfo.GetWorkers().Select(e  => e.Item1).ToList();
        wal = new DARQWal(me);
    }

    public bool ProcessMessage(DarqMessage m){
        Console.WriteLine("Got message");
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
                var storedProcedureName = Encoding.ASCII.GetString(m.GetMessageBody().ToArray());
                var schema = new (long, int)[]{};
                switch (storedProcedureName){
                    case "workloadA":
                        StoredProcedure p = StoredProcedure.GetWorkloadAUpdateHeavy(wal);
                        p.Run();
                        break;
                    default:
                        throw new NotImplementedException();
                }
                // execute stored procedure
                // listen to transaction, if state is validated, add writeset to log? 
                // requestBuilder.AddSelfMessage(BitConverter.GetBytes(0));

                var requestBuilder = new StepRequestBuilder(reusableRequest, me);
                requestBuilder.MarkMessageConsumed(m.GetLsn());
                requestBuilder.AddOutMessage(me, BitConverter.GetBytes(-1));
                m.Dispose();
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.SELF: // this is on recovery; TODO: do we need to double pass?
                // count = BitConverter.ToInt32(m.GetMessageBody());
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
}