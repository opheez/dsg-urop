using System;
using System.Diagnostics;

namespace DB 
{
public class ShardedBenchmark : TableBenchmark
{
    private int partitionId;
    private TransactionManager txnManager;
    private Table table;
    private CountdownEvent cde;
    internal int[] successCounts;
    public ShardedBenchmark(int partitionId, int numProcessors, string name, BenchmarkConfig cfg, ShardedTransactionManager txnManager, ShardedTable table, IWriteAheadLog? wal = null) : base(cfg) {
        System.Console.WriteLine("Init");
        this.partitionId = partitionId;
        this.txnManager = txnManager;
        this.table = table;
        this.wal = wal;
        Random r = new Random(cfg.seed);
        td = table.GetSchema();
        cde = new CountdownEvent(cfg.threadCount * cfg.perThreadDataCount);
        successCounts = new int[Math.Max(cfg.threadCount, cfg.perThreadDataCount)];

        // randomly assign reads and writes
        int numWrites = (int)(cfg.datasetSize * cfg.ratio);
        for (int i = 0; i < numWrites; i++){
            long index = r.NextInt64(cfg.datasetSize-1);
            // if this index is already a write, find the next available index
            if (isWrite[(int)index]) {
                while (isWrite[(int)index]){
                    index += 1;
                }
            }
            isWrite[(int)index] = true;
        }

        for (int i = 0; i < cfg.datasetSize; i++){
            values[i] = new byte[table.rowSize];
            r.NextBytes(values[i]);
        }

        for (int i = 0; i < cfg.datasetSize; i++){
            int key = (cfg.datasetSize * partitionId) + i;
            int randVal = r.Next(0, 100);
            if (randVal < 20) key += cfg.datasetSize * (randVal % numProcessors);
            keys[i] = new PrimaryKey(table.GetId(), key);
        }
        stats = new BenchmarkStatistics($"{name}-ShardedBenchmark", cfg, numWrites, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }
    
    override public void PopulateTables(){
        txnManager.Reset();
        txnManager.Run();
        var insertSw = Stopwatch.StartNew();
        InsertMultiThreadedTransactions(table, txnManager); // setup
        cde.Wait();
        insertSw.Stop();
        long insertMs = insertSw.ElapsedMilliseconds;
        stats?.AddTransactionalResult(ims: insertMs, insAborts: cfg.datasetSize - successCounts.Sum());
        System.Console.WriteLine("done inserting");
    }

    override public void RunTransactions(){
        cde.Reset(cfg.threadCount * cfg.perThreadDataCount);
        for (int i = 0; i < cfg.iterationCount; i++){
            var opSw = Stopwatch.StartNew();
            WorkloadMultiThreadedTransactions(table, txnManager, cfg.ratio);
            cde.Wait();
            opSw.Stop();
            long opMs = opSw.ElapsedMilliseconds;
            stats?.AddTransactionalResult(oms: opMs, txnAborts: cfg.datasetSize - successCounts.Sum());
            txnManager.Terminate();
        }
        // TODO: reset table and 
        stats?.ShowAllStats();
        stats?.SaveStatsToFile();
    }

    override protected internal int InsertSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        Action<bool, TransactionContext> incrementCount = (success, ctx) => {
            if (success) {
                if (ctx.tid % 10 == 0) stats?.AddLatencyResult(Stopwatch.GetElapsedTime(ctx.startTime).Milliseconds);
                Interlocked.Increment(ref successCounts[thread_idx]);
            }
            cde.Signal();
        };
        int abortCount = 0;
        int c = 0;
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++) {
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                tbl.Insert(ref keys[loc], values[loc], t);
            }
            txnManager.CommitWithCallback(t, incrementCount);
        }
        return abortCount;
    }

    override protected internal int WorkloadSingleThreadedTransactions(Table table, TransactionManager txnManager, int thread_idx, double ratio)
    {
        Action<bool, TransactionContext> incrementCount = (success, ctx) => {
            if (success) {
                if (ctx.tid % 10 == 0) stats?.AddLatencyResult(Stopwatch.GetElapsedTime(ctx.startTime).Milliseconds);
                Interlocked.Increment(ref successCounts[thread_idx]);
            }
            cde.Signal();
        };
        for (int i = 0; i < cfg.perThreadDataCount; i += 1){
            TransactionContext ctx = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++){
                int loc = i + j + (cfg.datasetSize / cfg.insertThreadCount * thread_idx);
                PrimaryKey key = keys[loc];

                if (isWrite[loc]) {
                    // shift value by thread_idx to write new value
                    int newValueIndex = loc + thread_idx < values.Length ?  loc + thread_idx : values.Length - 1;
                    byte[] val = values[newValueIndex];
                    table.Update(ref key, td, val, ctx);
                } else {
                    table.Read(key, td, ctx);
                }
            }
            txnManager.CommitWithCallback(ctx, incrementCount);
        }
        // return abortCount;

        // cde.Wait();
        // return abortCount;
        return 0;
    }
}
}