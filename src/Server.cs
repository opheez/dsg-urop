using DB;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using Grpc.Core;

/// <summary>
/// Server takes workload requests and orchestrates benchmark
/// 
/// </summary> 
public interface INode {
    public void EnqueueWorkload();
    public void GetBenchmarkResults();
    public void Start();
}

public struct ServerOptions {
    public IDarqClusterInfo ClusterInfo;
    public WorkerId Me;
    public long MinKey;
    public int Port;
}
public class Node {
    private ServerOptions options;

    public Node(ServerOptions options){
        this.options = options;
    }

    public void Start(){
        RunTransactionProcessorService();
        if (options.ClusterInfo != null){
            new Thread(() => RunDarqWithProcessor(options.Me, options.ClusterInfo)).Start();
        }
    }

    private void RunTransactionProcessorService(){
        var txnProcessor = new TransactionProcessorService(options.MinKey);

        var server = new Server
        {
            Services = { TransactionProcessor.BindService(txnProcessor) },
            Ports = { new ServerPort("localhost", options.Port, ServerCredentials.Insecure) }
        };
        server.Start();

        Console.WriteLine($"Server started on port {options.Port}");
    }

    private static void RunDarqWithProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
    {
        Console.WriteLine("Running DARQ with processor");
        var logDevice = new LocalStorageDevice($"C:\\Users\\Administrator\\Desktop\\data.log", deleteOnClose: true);
        var darqServer = new DarqServer(new DarqServerOptions
        {
            Port = 15721 + (int)me.guid,
            Address = "127.0.0.1",
            ClusterInfo = clusterInfo,
            me = me,
            DarqSettings = new DarqSettings
            {
                DprFinder = default,
                LogDevice = logDevice,
                PageSize = 1L << 22,
                MemorySize = 1L << 23,
                SegmentSize = 1L << 30,
                LogCommitManager = default,
                LogCommitDir = default,
                LogChecksum = LogChecksumType.None,
                MutableFraction = default,
                FastCommitMode = true,
                DeleteOnClose = true
            },
            commitIntervalMilli = 5,
            refreshIntervalMilli = 5
        });
        darqServer.Start();

        var processorClient = new ColocatedDarqProcessorClient(darqServer.GetDarq());
        processorClient.StartProcessingAsync(new DarqProcessor(me, clusterInfo)).GetAwaiter().GetResult();
        darqServer.Dispose();
        Console.WriteLine("DARQ server disposed?");
    }

}