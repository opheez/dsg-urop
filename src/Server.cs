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
public interface IServer {
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
public class ServerUsingDarq {
    private ServerOptions options;

    public ServerUsingDarq(ServerOptions options){
        this.options = options;
    }

    public void Start(){
        RunNodeService(options.Port, options.MinKey);
        if (options.ClusterInfo != null){
            new Thread(() => RunDarqWithProcessor(options.Me, options.ClusterInfo)).Start();
        }
    }

    private void RunNodeService(int port, long minKey){
        var node = new NodeService(minKey);

        var server = new Server
        {
            Services = { Node.BindService(node) },
            Ports = { new ServerPort("localhost", port, ServerCredentials.Insecure) }
        };
        server.Start();

        Console.WriteLine($"Server started on port {port}");
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
        processorClient.StartProcessingAsync(new DarqTransactionProcessor(me, clusterInfo)).GetAwaiter().GetResult();
        darqServer.Dispose();
        Console.WriteLine("DARQ server disposed?");
    }

}