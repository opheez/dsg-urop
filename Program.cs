using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
using darq;
using DB;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
unsafe class Program {

    public static int NumProcessors = 2;

    // public static void Main(){
    //     Console.WriteLine("Hello, World!");
    //     BenchmarkConfig ycsbCfg = new BenchmarkConfig(
    //         ratio: 0.2,
    //         seed: 12345,
    //         attrCount: 10,
    //         threadCount: 12,
    //         iterationCount: 3
    //     );
    //     TableBenchmark b = new FixedLenTableBenchmark("DictContext", ycsbCfg);
    //     b.RunTransactions();
    //     // b = new VarLenTableBenchmark(12345, 0.5);
    //     // b.Run();

    // }

    //     private static void RunDarqWithProcessor(DarqId me, IDarqClusterInfo clusterInfo)
    // {
    //     var logDevice = new LocalStorageDevice($"C:\\Users\\Administrator\\Desktop\\data.log", deleteOnClose: true);
    //     var darqServer = new DarqServer(new DarqServerOptions
    //     {
    //         Port = 15721 + (int)me.guid,
    //         Address = "127.0.0.1",
    //         ClusterInfo = clusterInfo,
    //         me = me,
    //         DarqSettings = new DarqSettings
    //         {
    //             DprFinder = default,
    //             LogDevice = logDevice,
    //             PageSize = 1L << 22,
    //             MemorySize = 1L << 23,
    //             SegmentSize = 1L << 30,
    //             LogCommitManager = default,
    //             LogCommitDir = default,
    //             LogChecksum = LogChecksumType.None,
    //             MutableFraction = default,
    //             FastCommitMode = true,
    //             DeleteOnClose = true
    //         },
    //         commitIntervalMilli = 5,
    //         refreshIntervalMilli = 5
    //     });
    //     darqServer.Start();
    //     // create grpc channels using clusterInfo ??

    //     var processorClient = new ColocatedDarqProcessorClient(darqServer.GetDarq());
    //     processorClient.StartProcessingAsync(new DarqProcessor(me, clusterInfo)).GetAwaiter().GetResult();
    //     darqServer.Dispose();
    // }

    // public static void Main(string[] args)
    // {
    //     // Compose cluster architecture first because clusterInfo is mutable type in NodeOptions struct
    //     var clusterInfo = new HardCodedClusterInfo();
    //     for (var i = 0; i < NumProcessors; i++)
    //     {
    //         clusterInfo.AddWorker(new DarqId(i), $"Test Worker {i}", "127.0.0.1", 15721 + i);
    //     }

    //     var threads = new List<Thread>();
    //     var nodes = new List<Node>();
    //     for (var i = 0; i < NumProcessors; i++) 
    //     {
    //         // Manually map services to ports and configure service provider
    //         Node node = new Node(new NodeOptions
    //         {
    //             Port = 50050 + i,
    //             MinKey = i * 1000,
    //             ClusterInfo = clusterInfo,
    //             Me = new DarqId(i)
    //         });
    //         nodes.Add(node);
        
    //         threads.Add(new Thread(() => node.Start()));
    //     }

    //     foreach (var t in threads)
    //         t.Start();

    //     nodes[0].EnqueueWorkload("a");

    //     // var darqClient = new DarqProducerClient(clusterInfo);
    //     // darqClient.EnqueueMessageAsync(new DarqId(0), Encoding.ASCII.GetBytes("workloadA"));

    //     // foreach (var t in threads)
    //     //     t.Join();
    // }

    public static void Main(string[] args)
    {
        LaunchService();

    }

    public static void LaunchService() {
        int me = 1;
        var builder = WebApplication.CreateBuilder();

        builder.Services.AddGrpc();
        // Other nodes to communicate with
        using var workerChannel = GrpcChannel.ForAddress("http://localhost:15722");
        var executors = new List<GrpcChannel> { workerChannel };
        builder.Services.AddSingleton(executors);
        // DARQ injection
        builder.Services.AddSingleton(new DarqBackgroundWorkerPoolSettings
        {
            numWorkers = 2
        });
        builder.Services.AddSingleton(new DarqSettings
        {
            LogDevice = new LocalStorageDevice($"C:\\Users\\Administrator\\Desktop\\data.log", deleteOnClose: true),
            PageSize = 1L << 22,
            MemorySize = 1L << 28,
            SegmentSize = 1L << 30,
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5,
            FastCommitMode = true,
            DeleteOnClose = true,
            CleanStart = true
        });
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<Darq>();
        builder.Services.AddSingleton<DarqBackgroundWorkerPool>();
        builder.Services.AddSingleton<IWriteAheadLog, DARQWal>(
            services => new DARQWal(new DarqId(me), services.GetRequiredService<Darq>(), services.GetRequiredService<List<GrpcChannel>>(), services.GetRequiredService<DarqBackgroundWorkerPool>())
        );
        builder.Services.AddSingleton<DarqProcessor>();

        var schema = new (long, int)[]{(12345,8)};
        builder.Services.AddSingleton(schema);
        builder.Services.AddSingleton<RpcClient>(_ => new RpcClient(1, new Dictionary<long, string>{
            {1, "http://localhost:5000"},
            {2, "http://localhost:5001"}
        }));
        builder.Services.AddSingleton<Table, ShardedTable>();
        builder.Services.AddSingleton<TransactionManager, ShardedTransactionManager>(services => new ShardedTransactionManager(1, services.GetRequiredService<IWriteAheadLog>()));

        builder.Services.AddSingleton<TransactionProcessorService>();


        var app = builder.Build();
        // Configure the HTTP request pipeline.
        app.MapGrpcService<TransactionProcessorService>();
        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        app.Run();
    }


}