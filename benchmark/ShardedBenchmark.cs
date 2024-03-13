using System;
using System.Diagnostics;

namespace DB 
{
public class ShardedBenchmark : TableBenchmark
{
    private TransactionManager txnManager;
    private Table table;

    private RpcClient rpcClient;
    public ShardedBenchmark(string name, BenchmarkConfig cfg, ShardedTransactionManager txnManager, ShardedTable table, IWriteAheadLog? wal = null) : base(cfg) {
        System.Console.WriteLine("Init");
        this.rpcClient = txnManager.GetRpcClient();
        this.txnManager = txnManager;
        this.table = table;
        this.wal = wal;
        Random r = new Random(cfg.seed);
        td = table.GetSchema();

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
            keys[i] = i;
        }
        stats = new BenchmarkStatistics($"{name}-ShardedBenchmark", cfg, numWrites, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }

    override public void RunTransactions(){
        for (int i = 0; i < cfg.iterationCount; i++){
            txnManager.Reset();
            txnManager.Run();
            // tables.Add(tbl.GetHashCode(), tbl);
            var insertSw = Stopwatch.StartNew();
            int insertAborts = InsertMultiThreadedTransactions(table, txnManager); // setup
            insertSw.Stop();
            System.Console.WriteLine("done inserting");
            long insertMs = insertSw.ElapsedMilliseconds;
            var opSw = Stopwatch.StartNew();
            int txnAborts = WorkloadMultiThreadedTransactions(table, txnManager, cfg.ratio);
            opSw.Stop();
            long opMs = opSw.ElapsedMilliseconds;
            stats?.AddTransactionalResult((insertMs, opMs, insertAborts, txnAborts));
            txnManager.Terminate();
        }
        // TODO: reset table and 
        stats?.ShowAllStats();
        stats?.SaveStatsToFile();
    }

}
}