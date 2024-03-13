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
        int sizeOfAttr = 10;
        Random r = new Random(cfg.seed);
        // Load data
        int offset = 0;
        for (int i = 0; i < cfg.attrCount; i++){
            long attr = r.NextInt64();
            schema[i] = (attr, sizeOfAttr);
            td[i] = new TupleDesc(attr, sizeOfAttr, offset);
            offset += sizeOfAttr;
        }

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
            values[i] = new byte[sizeOfAttr*cfg.attrCount];
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

    override protected internal int InsertSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int c = 0;
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++) {
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                if (rpcClient.IsLocalKey(keys[loc])) {
                    Console.WriteLine($"Inserting {keys[loc]}");
                    tbl.Insert(new TupleId(keys[loc], table), td, values[loc], t);
                }
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            } else {
                c++;
            }
        }
        return abortCount;
    }

}
}