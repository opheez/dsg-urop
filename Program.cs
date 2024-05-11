using System;
using System.Collections;
using System.Net;
using System.Text;
using BenchmarkDotNet.Running;
using darq;
using DB;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using FASTER.server;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
unsafe class Program {

    public static int NumProcessors = 2;
    public static int NComitterThreads = 2;
    public static int PartitionsPerThread = 2;
    public static int ThreadCount = 2;

    // public static void Main(){
    //     Console.WriteLine("Hello, World!");
    //     BenchmarkConfig ycsbCfg = new BenchmarkConfig(
    //         ratio: 0.2,
    //         seed: 12345,
    //         attrCount: 10,
    //         threadCount: ThreadCount,
    //         insertThreadCount: 12,
    //         nCommitterThreads: NComitterThreads,
    //         iterationCount: 1
    //     );
    //     // TableBenchmark b = new FixedLenTableBenchmark("DictContext", ycsbCfg);
    //     // b.RunTransactions();
    //     // b = new VarLenTableBenchmark(12345, 0.5);
    //     // b.Run();
    //     TpccConfig tpccConfig = new TpccConfig(
    //         numWh: ThreadCount * PartitionsPerThread,
    //         partitionsPerThread: PartitionsPerThread,
    //         newOrderCrossPartitionProbability: 0,
    //         paymentCrossPartitionProbability: 0
    //         // numCustomer: 10,
    //         // numDistrict: 10,
    //         // numItem: 10,
    //         // numOrder: 10,
    //         // numStock: 10
    //     );
    //     Dictionary<long, GrpcChannel> clusterMap = new Dictionary<long, GrpcChannel>();  
    //     TpccRpcClient rpcClient = new TpccRpcClient(0, clusterMap);

    //     Dictionary<int, ShardedTable> tables = new Dictionary<int, ShardedTable>();
    //     foreach (TableType tEnum in Enum.GetValues(typeof(TableType))){
    //         (long, int)[] schema;
    //         switch (tEnum) {
    //             case TableType.Warehouse:
    //                 schema = TpccSchema.WAREHOUSE_SCHEMA;
    //                 break;
    //             case TableType.District:
    //                 schema = TpccSchema.DISTRICT_SCHEMA;
    //                 break;
    //             case TableType.Customer:
    //                 schema = TpccSchema.CUSTOMER_SCHEMA;
    //                 break;
    //             case TableType.History:
    //                 schema = TpccSchema.HISTORY_SCHEMA;
    //                 break;  
    //             case TableType.Item:
    //                 schema = TpccSchema.ITEM_SCHEMA;
    //                 break;
    //             case TableType.NewOrder:
    //                 schema = TpccSchema.NEW_ORDER_SCHEMA;
    //                 break;
    //             case TableType.Order:
    //                 schema = TpccSchema.ORDER_SCHEMA;
    //                 break;
    //             case TableType.OrderLine:
    //                 schema = TpccSchema.ORDER_LINE_SCHEMA;
    //                 break;
    //             case TableType.Stock:
    //                 schema = TpccSchema.STOCK_SCHEMA;
    //                 break;
    //             default:
    //                 throw new Exception("Invalid table type");
    //         }
    //         int i = (int)tEnum;
    //         tables[i] = new ShardedTable(
    //             i,
    //             schema,
    //             rpcClient
    //         );
    //     }
    //     ShardedTransactionManager stm = new ShardedTransactionManager(
    //         NComitterThreads,
    //         rpcClient,
    //         tables
    //     );
        
    //     TpccBenchmark tpccBenchmark = new TpccBenchmark((int)0, tpccConfig, ycsbCfg, tables, stm);
    //     tpccBenchmark.RunTransactions();
    //     // tpccBenchmark.GenerateTables();
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
        // LaunchService(Int32.Parse(args[0]));
        LaunchService(0);
    }

    public static void LaunchService(int partitionId) {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddLogging(builder => builder.AddFilter(null, LogLevel.Error).AddConsole());
        // create channel to each server
        Dictionary<long, GrpcChannel> clusterMap = new Dictionary<long, GrpcChannel>();
        clusterMap[0] = GrpcChannel.ForAddress("http://10.1.0.4:5000");        
        clusterMap[1] = GrpcChannel.ForAddress("http://10.1.0.5:5000");        
        // clusterMap[2] = GrpcChannel.ForAddress("http://10.1.0.6:5000");        
        // clusterMap[3] = GrpcChannel.ForAddress("http://10.1.0.7:5000");       

        builder.Services.AddGrpc();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 5000,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        // DARQ injection
        builder.Services.AddSingleton<Dictionary<long, GrpcChannel>>(clusterMap);
        // builder.Services.AddSingleton<Dictionary<DarqId, GrpcChannel>>(clusterMap.ToDictionary(o => new DarqId(o.Key), o => o.Value));
        builder.Services.AddSingleton(
            new DarqSettings
            {
                LogDevice = new ManagedLocalStorageDevice($"/home/azureuser/data-{partitionId}.log", deleteOnClose: true),
                LogCommitDir = $"/home/azureuser/{partitionId}",
                DprFinder = new LocalStubDprFinder(),
                Me = new DarqId(partitionId),
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
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<Darq>());
        // builder.Services.AddSingleton<DarqBackgroundWorkerPool>(new DarqBackgroundWorkerPool(
        //     new DarqBackgroundWorkerPoolSettings
        //     {
        //         numWorkers = NumProcessors
        //     }
        // ));
        builder.Services.AddSingleton<DarqWal>(provider =>
            new DarqWal(
                new DarqId(partitionId),
                provider.GetRequiredService<Darq>(),
                provider.GetRequiredService<ILogger<DarqWal>>()
            ));
        Dictionary<int, ShardedTable> tables = new Dictionary<int, ShardedTable>();

        // // uncomment for YCSB
        // YcsbRpcClient rpcClient = new YcsbRpcClient(partitionId, clusterMap);
        // builder.Services.AddSingleton<YcsbRpcClient>(rpcClient);
        // var schema = TpccSchema.ITEM_SCHEMA;
        // tables[0] = new ShardedTable(0, schema, rpcClient);

        // uncomment for TPC-C
        TpccRpcClient rpcClient = new TpccRpcClient(partitionId, clusterMap);
        builder.Services.AddSingleton<TpccRpcClient>(rpcClient);
        foreach (TableType tEnum in Enum.GetValues(typeof(TableType))){
            (long, int)[] schema;
            switch (tEnum) {
                case TableType.Warehouse:
                    schema = TpccSchema.WAREHOUSE_SCHEMA;
                    break;
                case TableType.District:
                    schema = TpccSchema.DISTRICT_SCHEMA;
                    break;
                case TableType.Customer:
                    schema = TpccSchema.CUSTOMER_SCHEMA;
                    break;
                case TableType.History:
                    schema = TpccSchema.HISTORY_SCHEMA;
                    break;  
                case TableType.Item:
                    schema = TpccSchema.ITEM_SCHEMA;
                    break;
                case TableType.NewOrder:
                    schema = TpccSchema.NEW_ORDER_SCHEMA;
                    break;
                case TableType.Order:
                    schema = TpccSchema.ORDER_SCHEMA;
                    break;
                case TableType.OrderLine:
                    schema = TpccSchema.ORDER_LINE_SCHEMA;
                    break;
                case TableType.Stock:
                    schema = TpccSchema.STOCK_SCHEMA;
                    break;
                default:
                    throw new Exception("Invalid table type");
            }
            int i = (int)tEnum;
            tables[i] = new ShardedTable(
                i,
                schema,
                rpcClient
            );
        }
        
        
        builder.Services.AddSingleton<Dictionary<int, ShardedTable>>(tables);
        builder.Services.AddSingleton<ShardedTransactionManager>(provider => new ShardedTransactionManager(
            NComitterThreads,
            rpcClient,
            tables,
            wal: provider.GetRequiredService<DarqWal>(),
            logger: provider.GetRequiredService<ILogger<ShardedTransactionManager>>()
        ));

        BenchmarkConfig ycsbCfg = new BenchmarkConfig(
            ratio: 0.2,
            attrCount: 10,
            threadCount: ThreadCount,
            insertThreadCount: 12,
            iterationCount: 1,
            nCommitterThreads: NComitterThreads
            // perThreadDataCount: 100
        );
        TpccConfig tpccConfig = new TpccConfig(
            numWh: PartitionsPerThread * ThreadCount * NumProcessors,
            partitionsPerThread: PartitionsPerThread
            // newOrderCrossPartitionProbability: 0
            // paymentCrossPartitionProbability: 0
            // numCustomer: 10,
            // numDistrict: 10,
            // numItem: 10,
            // numOrder: 10,
            // numStock: 10
        );

        // TableBenchmark benchmark = new FixedLenTableBenchmark("ycsb_local", ycsbCfg, darqWal);
        // TableBenchmark benchmark = new ShardedBenchmark("2pc", ycsbCfg, stm, tables[0], darqWal);
        builder.Services.AddSingleton<TpccBenchmark>(provider => {
            TpccBenchmark benchmark = new TpccBenchmark((int)partitionId, tpccConfig, ycsbCfg, tables, provider.GetRequiredService<ShardedTransactionManager>());
            benchmark.PopulateTables();
            return benchmark;
        }
        );
        builder.Services.AddSingleton<DarqMaintenanceBackgroundService>(provider =>
            new DarqMaintenanceBackgroundService(
                provider.GetRequiredService<ILogger<DarqMaintenanceBackgroundService>>(),
                provider.GetRequiredService<Darq>(),
                new DarqMaintenanceBackgroundServiceSettings
                {
                    morselSize = 1024,
                    producerFactory = session => new TransactionProcessorProducerWrapper(clusterMap.ToDictionary(o => new DarqId(o.Key), o => o.Value), session),
                    speculative = true
                }
            )
        );

        builder.Services.AddSingleton<DarqTransactionBackgroundService>(provider =>
            new DarqTransactionBackgroundService(
                partitionId,
                tables,
                provider.GetRequiredService<ShardedTransactionManager>(),
                provider.GetRequiredService<DarqWal>(),
                provider.GetRequiredService<Darq>(),
                provider.GetRequiredService<TpccBenchmark>(),
                provider.GetRequiredService<ILogger<DarqTransactionBackgroundService>>()
            )
        );
        builder.Services.AddSingleton<DarqTransactionProcessorService>(provider =>
            new DarqTransactionProcessorService(
                provider.GetRequiredService<DarqTransactionBackgroundService>()
            )
        );
        builder.Services.AddSingleton<StateObjectRefreshBackgroundService>();
        
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>(provider =>
            provider.GetRequiredService<StateObjectRefreshBackgroundService>());
        builder.Services.AddHostedService<DarqMaintenanceBackgroundService>(provider =>
            provider.GetRequiredService<DarqMaintenanceBackgroundService>());
        builder.Services.AddHostedService<DarqTransactionBackgroundService>(provider =>
            provider.GetRequiredService<DarqTransactionBackgroundService>());

        // benchmark.PopulateTables();

        var app = builder.Build();
        // Configure the HTTP request pipeline.
        app.MapGrpcService<DarqTransactionProcessorService>();
        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        app.Run();
    }


}