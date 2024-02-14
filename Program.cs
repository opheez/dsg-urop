using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
using DB;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
unsafe class Program {

    public static int NumProcessors = 1;

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

        private static void RunDarqWithProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
    {
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
        // create grpc channels using clusterInfo ??

        var processorClient = new ColocatedDarqProcessorClient(darqServer.GetDarq());
        processorClient.StartProcessingAsync(new DarqTransactionProcessor(me, clusterInfo)).GetAwaiter().GetResult();
        darqServer.Dispose();
    }

    public static void Main(string[] args)
    {
        // var builder = WebApplication.CreateBuilder(args);
        // builder.Services.AddScoped<NodeService>();
        // builder.Services.AddGrpc();
        // var app = builder.Build();

        // // Configure the HTTP request pipeline.
        // app.MapGrpcService<NodeService>();
        // app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        // app.Run();

        // Compose cluster architecture first because clusterInfo is mutable type in ServerOptions struct
        var clusterInfo = new HardCodedClusterInfo();
        for (var i = 0; i < NumProcessors; i++)
        {
            clusterInfo.AddWorker(new WorkerId(i), $"Test Worker {i}", "127.0.0.1", 15721 + i);
        }

        var threads = new List<Thread>();
        for (var i = 0; i < NumProcessors; i++) 
        {
            // Manually map services to ports and configure service provider
            ServerUsingDarq server = new ServerUsingDarq(new ServerOptions
            {
                Port = 50050 + i,
                MinKey = i * 1000,
                ClusterInfo = clusterInfo,
                Me = new WorkerId(i)
            });
        
            threads.Add(new Thread(() => server.Start()));
        }

        foreach (var t in threads)
            t.Start();

        // var darqClient = new DarqProducerClient(clusterInfo);
        // darqClient.EnqueueMessageAsync(new WorkerId(0), Encoding.ASCII.GetBytes("workloadA"));

        // foreach (var t in threads)
        //     t.Join();
    }


}