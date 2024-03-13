using System;
using System.Threading;
using System.Diagnostics;
using System.Collections;

namespace DB {

public struct BenchmarkConfig {
    public int seed;
    public double ratio;
    public int threadCount;
    public int attrCount;
    public int perThreadDataCount; // enough to be larger than l3 cache
    public int iterationCount;
    public int datasetSize;
    public int perTransactionCount; // TODO: ensure support for keys[] when this > 1
    public int nCommitterThreads;

    public BenchmarkConfig(
        int seed = 12345,
        double ratio = 0.2,
        int threadCount = 16,
        int attrCount = 2,
        int perThreadDataCount = 100000,
        int iterationCount = 10,
        int perTransactionCount = 1,
        int nCommitterThreads = 7){

        this.seed = seed;
        this.ratio = ratio;
        this.threadCount = threadCount;
        this.attrCount = attrCount;
        this.perThreadDataCount = perThreadDataCount;
        this.iterationCount = iterationCount;
        this.perTransactionCount = perTransactionCount;
        this.datasetSize = perThreadDataCount * threadCount;
        this.nCommitterThreads = nCommitterThreads;
    }

    public override readonly string ToString(){
        return $"seed: {seed}, ratio: {ratio}, threadCount: {threadCount}, attrCount: {attrCount}, perThreadDataCount: {perThreadDataCount}, iterationCount: {iterationCount}, perTransactionCount: {perTransactionCount}, datasetSize: {datasetSize}, nCommitterThreads: {nCommitterThreads}";
    }
}

public abstract class TableBenchmark
{
    protected internal BenchmarkConfig cfg;   
    protected internal long[] keys;
    protected internal byte[][] values;
    protected internal BitArray isWrite;
    // TODO: in the future support multiple tables, make this list
    protected internal TupleDesc[] td; // TODO: this is the same as schema
    protected internal Thread[] workers;
    protected internal BenchmarkStatistics? stats;
    protected internal IWriteAheadLog? wal;

    public TableBenchmark(BenchmarkConfig cfg, IWriteAheadLog? wal = null){
        this.cfg = cfg;
        this.wal = wal;
        td = new TupleDesc[cfg.attrCount];
        workers = new Thread[cfg.threadCount];

        keys = new long[cfg.datasetSize];
        values = new byte[cfg.datasetSize][];
        isWrite = new BitArray(cfg.datasetSize);
    }

    // internal void InsertSingleThreaded(Table tbl, int thread_idx){
    //     for (int i = 0; i < cfg.perThreadDataCount; i++){
    //         int loc = i + (cfg.perThreadDataCount * thread_idx);
    //         tbl.Insert(new KeyAttr(keys[loc],attrs[loc%cfg.attrCount], tbl), values[loc]);
    //     }
    // }

    virtual protected internal int InsertSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int c = 0;
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++) {
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                TupleId tupleId = tbl.Insert(td, values[loc], t);
                keys[loc] = tupleId.Key;
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

    // internal void InsertMultiThreaded(Table tbl)
    // {
    //     for (int thread = 0; thread < cfg.threadCount; thread++) {
    //         int t = thread;
    //         workers[thread] = new Thread(() => InsertSingleThreaded(tbl, t));
    //         workers[thread].Start();
    //     }
    //     for (int thread = 0; thread < cfg.threadCount; thread++) {
    //         workers[thread].Join();
    //     }
    // }

    protected internal int InsertMultiThreadedTransactions(Table tbl, TransactionManager txnManager)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = InsertSingleThreadedTransactions(tbl, txnManager, t);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    // internal void WorkloadSingleThreaded(Table tbl, int thread_idx, double ratio){
    //     for (int i = 0; i < cfg.perThreadDataCount; i++){
    //         int loc = i + (cfg.perThreadDataCount * thread_idx);
    //         if (keys[loc] < Int64.MaxValue * ratio) {
    //             tbl.Update(new KeyAttr(keys[loc+DatasetSize],attrs[loc%cfg.attrCount], tbl),values[loc]);
    //         } else {
    //             tbl.Read(new KeyAttr(keys[loc], attrs[loc%cfg.attrCount], tbl));
    //         }
    //     }
    // }

    protected internal int WorkloadSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx, double ratio){
        int abortCount = 0;
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++){
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                long key = keys[loc];
                // uncomment to make workload only insert one attribute instead of all
                // long attr = schema[loc%cfg.attrCount].Item1;
                // TupleDesc[] td = new TupleDesc[]{new TupleDesc(attr, tbl.metadata[attr].Item1)};

                if (isWrite[loc]) {
                    // shift value by thread_idx to write new value
                    int newValueIndex = loc + thread_idx < values.Length ?  loc + thread_idx : values.Length - 1;
                    // Span<byte> val = new Span<byte>(values[newValueIndex]).Slice(0, sizeof(long));
                    byte[] val = values[newValueIndex];
                    tbl.Update(new TupleId(key, tbl), td, val, t);
                } else {
                    tbl.Read(new TupleId(key, tbl), td, t);
                }
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            }
        }
        return abortCount;
    }

    // internal void WorkloadMultiThreaded(Table tbl, double ratio)
    // {
    //     for (int thread = 0; thread < cfg.threadCount; thread++) {
    //         int t = thread;
    //         workers[thread] = new Thread(() => WorkloadSingleThreaded(tbl, t, ratio));
    //         workers[thread].Start();
    //     }
    //     for (int thread = 0; thread < cfg.threadCount; thread++) {
    //         workers[thread].Join();
    //     }
    // }

    protected internal int WorkloadMultiThreadedTransactions(Table tbl, TransactionManager txnManager, double ratio)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = WorkloadSingleThreadedTransactions(tbl, txnManager, t, ratio);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }


    // public void Run(){
    //     for (int i = 0; i < IterationCount; i++){
    //         using (Table tbl = new Table(schema)) {
    //             var insertSw = Stopwatch.StartNew();
    //             InsertMultiThreaded(tbl); // setup
    //             insertSw.Stop();
    //             long insertMs = insertSw.ElapsedMilliseconds;
    //             var opSw = Stopwatch.StartNew();
    //             WorkloadMultiThreaded(tbl, ratio);
    //             opSw.Stop();
    //             long opMs = opSw.ElapsedMilliseconds;
    //             stats?.AddResult((insertMs, opMs));
    //         }
    //     }
    //     stats?.ShowAllStats();
    //     stats?.SaveStatsToFile();
    // }

    // public void RunTransactions(ref Dictionary<int, Table> tables){
    virtual public void RunTransactions(){
        for (int i = 0; i < cfg.iterationCount; i++){
            TransactionManager txnManager = new TransactionManager(cfg.nCommitterThreads, wal);
            txnManager.Run();
            (long, int)[] schema = new (long, int)[cfg.attrCount];
            for (int j = 0; j < td.Length; j++){
                schema[j] = (td[j].Attr, td[j].Size);
            }
            using (Table tbl = new Table(1, schema)) {
                // tables.Add(tbl.GetHashCode(), tbl);
                var insertSw = Stopwatch.StartNew();
                int insertAborts = InsertMultiThreadedTransactions(tbl, txnManager); // setup
                insertSw.Stop();
                System.Console.WriteLine("done inserting");
                long insertMs = insertSw.ElapsedMilliseconds;
                var opSw = Stopwatch.StartNew();
                int txnAborts = WorkloadMultiThreadedTransactions(tbl, txnManager, cfg.ratio);
                opSw.Stop();
                long opMs = opSw.ElapsedMilliseconds;
                stats?.AddTransactionalResult((insertMs, opMs, insertAborts, txnAborts));
            }
            txnManager.Terminate();
        }
        stats?.ShowAllStats();
        stats?.SaveStatsToFile();
    }
}

public class BenchmarkStatistics {
    internal readonly List<long> insMsPerRun = new List<long>();
    internal readonly List<long> opsMsPerRun = new List<long>();
    internal readonly List<int> insAbortsPerRun = new List<int>();
    internal readonly List<int> txnAbortsPerRun = new List<int>();
    internal string name;
    internal BenchmarkConfig cfg;
    internal int inserts;
    internal int operations;

    internal BenchmarkStatistics(string name, BenchmarkConfig cfg, int inserts, int operations)
    {
        this.cfg = cfg;
        this.name = name;
        this.inserts = inserts;
        this.operations = operations;
    }

    internal void AddResult((long ims, long oms) result)
    {
        insMsPerRun.Add(result.ims);
        opsMsPerRun.Add(result.oms);
    }

    internal void AddTransactionalResult((long ims, long oms, int insAborts, int txnAborts) result)
    {
        insMsPerRun.Add(result.ims);
        opsMsPerRun.Add(result.oms);
        insAbortsPerRun.Add(result.insAborts);
        txnAbortsPerRun.Add(result.txnAborts);
    }

    internal string[] GetStats() {
        string[] data = new string[]{
            $"Benchmark {name}",
            "-----BENCHMARK CONFIG-----",
            cfg.ToString(),
            "-----STATS-----",
            GetInsDataString(operations, insMsPerRun),
            GetOpsDataString(inserts, operations-inserts, opsMsPerRun)
        };

        if (insAbortsPerRun.Count != 0) {
            data = data.Concat(new string[]{
                GetInsAbortDataString(insAbortsPerRun),
                GetTxnAbortDataString(txnAbortsPerRun)
            }).ToArray();
        }

        return data;
    }

    internal void ShowAllStats(){
        foreach (string line in GetStats()){
            Console.WriteLine(line);
        }
    }

    internal async void SaveStatsToFile(){
        string now = DateTime.Now.ToString("yyyyMMdd-HHmmss"); 
        await File.WriteAllLinesAsync($"benchmark/benchmarkResults/{name}-{now}.txt", GetStats());
    }

    internal string GetOpsDataString(int inserts, int reads, List<long> opsMsPerRun) => $"{(inserts+reads)/opsMsPerRun.Average()} operations/ms ({inserts+reads} operations ({inserts} inserts, {reads} reads) in {opsMsPerRun.Average()} ms)";
    internal string GetInsDataString(int inserts, List<long> insMsPerRun) => $"{inserts/insMsPerRun.Average()} inserts/ms ({inserts} inserts in {insMsPerRun.Average()} ms)";
    internal string GetTxnAbortDataString(List<int> txnAborts) => $"Operations: Average {txnAborts.Average()} aborts out of {cfg.perThreadDataCount/cfg.perTransactionCount} transactions ({txnAborts.Average()/(cfg.perThreadDataCount/cfg.perTransactionCount)}% abort rate)";
    internal string GetInsAbortDataString(List<int> insAborts) => $"Insertions: Average {insAborts.Average()} aborts out of {cfg.perThreadDataCount/cfg.perTransactionCount} transactions ({insAborts.Average()/(cfg.perThreadDataCount/cfg.perTransactionCount)}% abort rate)";

    // internal static string GetLoadingTimeLine(double insertsPerSec, long elapsedMs)
    //     => $"##00; {InsPerSec}: {insertsPerSec:N2}; sec: {(double)elapsedMs / 1000:N3}";

    // internal static string GetAddressesLine(AddressLineNum lineNum, long begin, long head, long rdonly, long tail)
    //     => $"##{(int)lineNum:00}; begin: {begin}; head: {head}; readonly: {rdonly}; tail: {tail}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string opsPerSecTag, double opsPerSec)
    //     => $"##{(int)lineNum:00}; {opsPerSecTag}: {opsPerSec:N2}; {OptionsString}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string meanTag, double mean, double stdev, double stdevpct)
    //     => $"##{(int)lineNum:00}; {meanTag}: {mean:N2}; stdev: {stdev:N1}; stdev%: {stdevpct:N1}; {OptionsString}";
}

}