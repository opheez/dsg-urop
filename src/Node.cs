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
    public void EnqueueWorkload(string workloadType);
    public void GetBenchmarkResults();
    public void Start();
}

public struct NodeOptions {
    public IDarqClusterInfo ClusterInfo;
    public WorkerId Me;
    public long MinKey;
    public int Port;
    public int nCommitterThreads;
}
public class Node : INode {
    private NodeOptions options;
    private DarqProcessor darqProcessor;
    private RpcService txnProcessor;
    private Table table;
    private TransactionManager txnManager;
    // TODO: benchmark data also 

    public Node(NodeOptions options){
        this.options = options;
    }

    public void Start(){
        // TODO: hard coded for now, need to define schema
        var schema = new (long, int)[]{(12345,8)};
        var wal = new BatchDARQWal(options.Me);

        txnManager = new TransactionManager(options.nCommitterThreads, wal);
        RpcClient client = new RpcClient(options.Me, options.ClusterInfo);
        table = new ShardedTable(schema, client);
        darqProcessor = new DarqProcessor(options.Me, options.ClusterInfo, wal);

        txnManager.Run();
        RunRpcService();
        if (options.ClusterInfo != null){
            new Thread(() => RunDarqWithProcessor(options.Me, options.ClusterInfo)).Start();
        }
    }

    public void Dispose(){
        table.Dispose();
        txnManager.Terminate();
    }

    public void EnqueueWorkload(string workloadType){
        table.Write(new KeyAttr(options.Me.guid, 12345, table), new byte[]{1,2,3,4,5,6,7,8});

        var ctx = txnManager.Begin();
        Console.WriteLine("Should go to own");
        var own = table.Read(new TupleId(0, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        Console.WriteLine(own.ToString());
        foreach (var b in own.ToArray()){
            Console.WriteLine(b);
        }
        Console.WriteLine("Should RPC:");
        var other = table.Read(new TupleId(1, table), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        Console.WriteLine(other.ToString());
        foreach (var b in other.ToArray()){
            Console.WriteLine(b);
        }


        // const int iterationCount = 1;
        // for (int i = 0; i < iterationCount; i++){
        //     table.Read(new TupleId());
        //     // TODO: reset table and txnManager
        // }
        // throw new NotImplementedException();
        // call benchmark here, creates table and txnmanager
        // spawn multiple threads which each
        // call read/write 
    }

    public void GetBenchmarkResults(){
        throw new NotImplementedException();
    }

    private void RunRpcService(){
        txnProcessor = new RpcService(table, txnManager, options.MinKey);

        var server = new Server
        {
            Services = { TransactionProcessor.BindService(txnProcessor) },
            Ports = { new ServerPort("localhost", options.Port, ServerCredentials.Insecure) }
        };
        server.Start();

        Console.WriteLine($"Server started on port {options.Port}");
    }

    private void RunDarqWithProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
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
        processorClient.StartProcessingAsync(darqProcessor).GetAwaiter().GetResult();
        darqServer.Dispose();
        Console.WriteLine("DARQ server disposed?");
    }

}