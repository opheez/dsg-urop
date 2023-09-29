using System;
using System.Threading;
using System.Diagnostics;

namespace DB {

public abstract class TableBenchmark
{
    public static int PerThreadDataCount = 100000; // enough to be larger than l3 cache
    public static int PerTransactionCount = 1; // TODO: get rid of, or ensure support for keys[] when this > 1
    internal static int ThreadCount = 16;
    internal static int AttrCount = 2;
    internal static int DatasetSize = PerThreadDataCount * ThreadCount;    
    internal long[] keys;
    internal long[] attrs;
    internal byte[][] values;
    internal (long, int)[] schema;
    internal Thread[] workers;
    internal BenchmarkStatistics stats;
    internal int IterationCount = 10;
    internal double ratio;
    internal int seed;
    internal LogWAL logWal;

    public TableBenchmark(int seed, double ratio){
        schema = new (long, int)[AttrCount];
        workers = new Thread[ThreadCount];
        this.ratio = ratio;
        this.seed = seed;

        keys = new long[DatasetSize];
        attrs = new long[AttrCount];
        values = new byte[DatasetSize][];
    }

    public void WithLogWAL(LogWAL logWal){
        this.logWal = logWal;
    }


    // internal void InsertSingleThreaded(Table tbl, int thread_idx){
    //     for (int i = 0; i < PerThreadDataCount; i++){
    //         int loc = i + (PerThreadDataCount * thread_idx);
    //         tbl.Insert(new KeyAttr(keys[loc],attrs[loc%AttrCount], tbl), values[loc]);
    //     }
    // }

    internal int InsertSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int c = 0;
        for (int i = 0; i < PerThreadDataCount; i += PerTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < PerTransactionCount; j++) {
                int loc = i + j + (PerThreadDataCount * thread_idx);
                long attr = attrs[loc%AttrCount];
                // TODO: different for varlen
                TupleDesc[] td = new TupleDesc[]{new TupleDesc(attr, tbl.metadata[attr].Item1)};
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
    //     for (int thread = 0; thread < ThreadCount; thread++) {
    //         int t = thread;
    //         workers[thread] = new Thread(() => InsertSingleThreaded(tbl, t));
    //         workers[thread].Start();
    //     }
    //     for (int thread = 0; thread < ThreadCount; thread++) {
    //         workers[thread].Join();
    //     }
    // }

    internal int InsertMultiThreadedTransactions(Table tbl, TransactionManager txnManager)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = InsertSingleThreadedTransactions(tbl, txnManager, t);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    // internal void WorkloadSingleThreaded(Table tbl, int thread_idx, double ratio){
    //     for (int i = 0; i < PerThreadDataCount; i++){
    //         int loc = i + (PerThreadDataCount * thread_idx);
    //         if (keys[loc] < Int64.MaxValue * ratio) {
    //             tbl.Update(new KeyAttr(keys[loc+DatasetSize],attrs[loc%AttrCount], tbl),values[loc]);
    //         } else {
    //             tbl.Read(new KeyAttr(keys[loc], attrs[loc%AttrCount], tbl));
    //         }
    //     }
    // }

    internal int WorkloadSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx, double ratio){
        int abortCount = 0;
        int groupSize = (int)(1/(1-ratio));
        for (int i = 0; i < PerThreadDataCount; i += PerTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < PerTransactionCount; j++){
                int loc = i + j + PerThreadDataCount;
                long attr = attrs[loc%AttrCount];
                long key = keys[loc];
                TupleDesc[] td = new TupleDesc[]{new TupleDesc(attr, tbl.metadata[attr].Item1)};

                if (loc % groupSize == 0) {
                    tbl.Update(new TupleId(key, tbl.GetHashCode()), td, values[loc], t);
                } else {
                    tbl.Read(new TupleId(key, tbl.GetHashCode()), td, t);
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
    //     for (int thread = 0; thread < ThreadCount; thread++) {
    //         int t = thread;
    //         workers[thread] = new Thread(() => WorkloadSingleThreaded(tbl, t, ratio));
    //         workers[thread].Start();
    //     }
    //     for (int thread = 0; thread < ThreadCount; thread++) {
    //         workers[thread].Join();
    //     }
    // }

    internal int WorkloadMultiThreadedTransactions(Table tbl, TransactionManager txnManager, double ratio)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = WorkloadSingleThreadedTransactions(tbl, txnManager, t, ratio);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
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
    //             stats.AddResult((insertMs, opMs));
    //         }
    //     }
    //     stats.ShowAllStats();
    //     stats.SaveStatsToFile();
    // }

    public void RunTransactions(){
        for (int i = 0; i < IterationCount; i++){
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();
            using (Table tbl = new Table(schema, logWal)) {
                var insertSw = Stopwatch.StartNew();
                int insertAborts = InsertMultiThreadedTransactions(tbl, txnManager); // setup
                insertSw.Stop();
                System.Console.WriteLine("done inserting");
                long insertMs = insertSw.ElapsedMilliseconds;
                var opSw = Stopwatch.StartNew();
                int txnAborts = WorkloadMultiThreadedTransactions(tbl, txnManager, ratio);
                opSw.Stop();
                long opMs = opSw.ElapsedMilliseconds;
                stats.AddTransactionalResult((insertMs, opMs, insertAborts, txnAborts));
            }
            txnManager.Terminate();
        }
        stats.ShowAllStats();
        stats.SaveStatsToFile();
    }
}

public class BenchmarkStatistics {
    internal readonly List<long> insMsPerRun = new List<long>();
    internal readonly List<long> opsMsPerRun = new List<long>();
    internal readonly List<int> insAbortsPerRun = new List<int>();
    internal readonly List<int> txnAbortsPerRun = new List<int>();
    internal string name;
    internal int inserts;
    internal int operations;

    internal BenchmarkStatistics(string name, int inserts, int operations)
    {
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

    internal void ShowAllStats(){
        System.Console.WriteLine("-----STATS-----");
        Console.WriteLine(GetInsDataString(operations, insMsPerRun));
        Console.WriteLine(GetOpsDataString(inserts, operations-inserts, opsMsPerRun));
        if (insAbortsPerRun.Count != 0){
            Console.WriteLine(GetInsAbortDataString(insAbortsPerRun));
            Console.WriteLine(GetTxnAbortDataString(txnAbortsPerRun));
        }
    }

    internal async void SaveStatsToFile(){
        System.Console.WriteLine("-----STATS-----");
        string[] data = new string[]{
            GetInsDataString(operations, insMsPerRun),
            GetOpsDataString(inserts, operations-inserts, opsMsPerRun)
        }.Concat(insAbortsPerRun.Count != 0 ? new string[]{
            GetInsAbortDataString(insAbortsPerRun),
            GetTxnAbortDataString(txnAbortsPerRun)
        } : new string[0]).ToArray();
        string now = DateTime.Now.ToString("yyyyMMdd-HHmmss"); 
        await File.WriteAllLinesAsync($"benchmark/benchmarkResults/{name}-{now}.txt", data);
    }

    internal static string GetOpsDataString(int inserts, int reads, List<long> opsMsPerRun) => $"{(inserts+reads)/opsMsPerRun.Average()} operations/ms ({inserts+reads} operations ({inserts} inserts, {reads} reads) in {opsMsPerRun.Average()} ms)";
    internal static string GetInsDataString(int inserts, List<long> insMsPerRun) => $"{inserts/insMsPerRun.Average()} inserts/ms ({inserts} inserts in {insMsPerRun.Average()} ms)";
    internal static string GetTxnAbortDataString(List<int> txnAborts) => $"Operations: Average {txnAborts.Average()} aborts out of {TableBenchmark.PerThreadDataCount/TableBenchmark.PerTransactionCount} transactions ({txnAborts.Average()/(TableBenchmark.PerThreadDataCount/TableBenchmark.PerTransactionCount)}% abort rate)";
    internal static string GetInsAbortDataString(List<int> insAborts) => $"Insertions: Average {insAborts.Average()} aborts out of {TableBenchmark.PerThreadDataCount/TableBenchmark.PerTransactionCount} transactions ({insAborts.Average()/(TableBenchmark.PerThreadDataCount/TableBenchmark.PerTransactionCount)}% abort rate)";

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