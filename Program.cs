﻿using System;
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
        LaunchService(Int32.Parse(args[0]));

    }

    public static void LaunchService(int me) {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddLogging(builder => builder.AddFilter(null, LogLevel.Error).AddConsole());
        // create channel to each server
        Dictionary<long, GrpcChannel> clusterMap = new Dictionary<long, GrpcChannel>();
        for (int i = 0; i < NumProcessors; i++){
            string address = "http://" + IPAddress.Loopback.ToString() + ":" + (5000 + i);
            clusterMap[i] = GrpcChannel.ForAddress(address);
            Console.WriteLine($"Created channel to {address}");
        }

        builder.Services.AddGrpc();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Loopback, 5000 + me,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        // DARQ injection
        builder.Services.AddSingleton<Dictionary<long, GrpcChannel>>(clusterMap);
        builder.Services.AddSingleton<Dictionary<DarqId, GrpcChannel>>(clusterMap.ToDictionary(o => new DarqId(o.Key), o => o.Value));
        builder.Services.AddSingleton(new DarqBackgroundWorkerPoolSettings
        {
            numWorkers = 2
        });
        builder.Services.AddSingleton(new DarqSettings
        {
            LogDevice = new LocalStorageDevice($"C:\\Users\\Administrator\\Desktop\\data-{me}.log", deleteOnClose: true),
            LogCommitDir = $"C:\\Users\\Administrator\\Desktop\\{me}",
            Me = new DarqId(me),
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
        builder.Services.AddSingleton<DarqWal>(
            services => new DarqWal(new DarqId(me), services.GetRequiredService<ILogger<DarqWal>>())
        );

        builder.Services.AddSingleton<RpcClient>(_ => new RpcClient(me, clusterMap));
        builder.Services.AddSingleton<Dictionary<int, ShardedTable>>(
            services => {
                Dictionary<int, ShardedTable> tables = new Dictionary<int, ShardedTable>();
                foreach (TableType tEnum in Enum.GetValues(typeof(TableType))){
                    (long, int, Type)[] schema = new (long, int, Type)[0];
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
                        schema.Select(x => (x.Item1, x.Item2)).ToArray(),
                        services.GetRequiredService<RpcClient>(),
                        services.GetRequiredService<ILogger<ShardedTable>>()
                    );
                }
                
                return tables;
            }
        );
        builder.Services.AddSingleton<ShardedTransactionManager>(
            services => new ShardedTransactionManager(1,
                            services.GetRequiredService<DarqWal>(),
                            services.GetRequiredService<RpcClient>(),
                            services.GetRequiredService<Dictionary<int, ShardedTable>>(),
                            services.GetRequiredService<ILogger<ShardedTransactionManager>>()
                            ));

        builder.Services.AddSingleton<DarqTransactionProcessorService>(
            service => new DarqTransactionProcessorService(
                me,
                service.GetRequiredService<Dictionary<int, ShardedTable>>(),
                service.GetRequiredService<ShardedTransactionManager>(),
                service.GetRequiredService<DarqWal>(),
                service.GetRequiredService<Darq>(),
                service.GetRequiredService<DarqBackgroundWorkerPool>(),
                service.GetRequiredService<Dictionary<DarqId, GrpcChannel>>(),
                service.GetRequiredService<ILogger<DarqTransactionProcessorService>>()
            )
        );

        var app = builder.Build();
        // Configure the HTTP request pipeline.
        app.MapGrpcService<DarqTransactionProcessorService>();
        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        app.Run();
    }


}