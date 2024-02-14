using System;
using System.Text;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using System.Diagnostics;
using Grpc.Core;

namespace DB {

// Takes in requests for stored procedures and executes them
public class DarqProcessor : IDarqProcessor {
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private List<WorkerId> workers;
    private StepRequest reusableRequest = new(null);
    private BatchDARQWal wal;
    Dictionary<int, Table> tables = new Dictionary<int, Table>();

    
    public DarqProcessor(WorkerId me, IDarqClusterInfo clusterInfo){
        this.me = me;
        workers = clusterInfo.GetWorkers().Select(e  => e.Item1).ToList();
        wal = new BatchDARQWal(me);
    }

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
                var storedProcedureName = Encoding.ASCII.GetString(m.GetMessageBody().ToArray());
                var schema = new (long, int)[]{};
                switch (storedProcedureName){
                    case "workloadA":
                        // StoredProcedure p = StoredProcedure.GetWorkloadAUpdateHeavy(wal);
                        // p.Run();'
                        BenchmarkConfig ycsbCfg = new BenchmarkConfig(
                            ratio: 0.2,
                            seed: 12345,
                            attrCount: 10,
                            threadCount: 12,
                            iterationCount: 3
                        );
                        TableBenchmark b = new FixedLenTableBenchmark("OneAttr", ycsbCfg, wal);
                        b.RunTransactions();
                        break;
                    default:
                        throw new NotImplementedException();
                }

                var requestBuilder = new StepRequestBuilder(reusableRequest, me);
                requestBuilder.MarkMessageConsumed(m.GetLsn());
                requestBuilder.AddOutMessage(me, BitConverter.GetBytes(-1));
                m.Dispose();
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.SELF: // this is on recovery; TODO: do we need to double pass?
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
}