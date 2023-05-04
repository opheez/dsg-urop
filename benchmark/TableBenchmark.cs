using System;
using System.Threading;
using System.Diagnostics;
using DB;
public abstract class TableBenchmark
{
    public static int PerThreadDataCount = 100000; // enough to be larger than l3 cache
    public static int PerTransactionCount = 5;
    internal static int ThreadCount = 2;
    internal static int AttrCount = 8;
    internal static int DatasetSize = PerThreadDataCount * ThreadCount;    
    internal static long[] keys;
    internal static long[] attrs;
    internal static byte[][] values;
    internal static Dictionary<long,(bool,int)> schema;
    internal static Thread[] workers;
    internal BenchmarkStatistics stats;
    internal int IterationCount = 1;
    internal double ratio;
    internal int seed;
    public TableBenchmark(int seed, double ratio){
        schema = new Dictionary<long, (bool,int)>();
        workers = new Thread[ThreadCount];
        this.ratio = ratio;
        this.seed = seed;

        keys = new long[DatasetSize+DatasetSize];
        attrs = new long[AttrCount];
        values = new byte[DatasetSize][];
    }

    internal void MultiThreadedUpserts(Table tbl)
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => Upserts(tbl, t));
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
    }

    internal int MultiThreadedUpsertTransactions(Table tbl, TransactionManager txnManager)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = UpsertTransactions(tbl, txnManager, t);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    internal void MultiThreadedUpsertsReads(Table tbl, double ratio)
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => UpsertsReads(tbl, t, ratio));
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
    }

    internal int MultiThreadedTransactions(Table tbl, TransactionManager txnManager, double ratio)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < ThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = Transactions(tbl, txnManager, t, ratio);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    internal void Upserts(Table tbl, int thread_idx){
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i + (PerThreadDataCount * thread_idx);
            tbl.Upsert(keys[loc],attrs[loc%AttrCount],values[loc]);
        }
    }

    internal int UpsertTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int c = 0;
        for (int i = 0; i < PerThreadDataCount; i += PerTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < PerTransactionCount; j++) {
                int loc = i + j + (PerThreadDataCount * thread_idx);
                tbl.Upsert(new KeyAttr(keys[loc], attrs[loc%AttrCount], tbl), values[loc].AsSpan(), t);
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            } else {
                c++;
            }
            // System.Console.WriteLine(c);
            // System.Console.WriteLine($"{thread_idx}: {c}");
        }
        System.Console.WriteLine(abortCount);
        return abortCount;
    }

    internal void UpsertsReads(Table tbl, int thread_idx, double ratio){
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i + (PerThreadDataCount * thread_idx);
            if (keys[loc] < Int64.MaxValue * ratio) {
                tbl.Upsert(keys[loc+DatasetSize],attrs[loc%AttrCount],values[loc]);
            } else {
                tbl.Read(keys[loc], attrs[loc%AttrCount]);
            }
        }
    }

    internal int Transactions(Table tbl, TransactionManager txnManager, int thread_idx, double ratio){
        int abortCount = 0;
        for (int i = 0; i < PerThreadDataCount; i += PerTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < PerTransactionCount; j++){
                int loc = i + j + (PerThreadDataCount * thread_idx);
                if (keys[loc] < Int64.MaxValue * ratio) {
                    tbl.Upsert(new KeyAttr(keys[loc+DatasetSize],attrs[loc%AttrCount], tbl), values[loc].AsSpan(), t);
                } else {
                    tbl.Read(new KeyAttr(keys[loc],attrs[loc%AttrCount], tbl), t);
                }
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            }
        }
        return abortCount;
    }

    public void Run(){
        for (int i = 0; i < IterationCount; i++){
            using (Table tbl = new Table(schema)) {
                var insertSw = Stopwatch.StartNew();
                MultiThreadedUpserts(tbl); // setup
                insertSw.Stop();
                long insertMs = insertSw.ElapsedMilliseconds;
                var opSw = Stopwatch.StartNew();
                MultiThreadedUpsertsReads(tbl, ratio);
                opSw.Stop();
                long opMs = opSw.ElapsedMilliseconds;
                stats.AddResult((insertMs, opMs));
            }
        }
        stats.ShowAllStats();
        stats.SaveStatsToFile();
    }

    public void RunTransactions(){
        for (int i = 0; i < IterationCount; i++){
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();
            using (Table tbl = new Table(schema)) {
                var insertSw = Stopwatch.StartNew();
                int insertAborts = MultiThreadedUpsertTransactions(tbl, txnManager); // setup
                insertSw.Stop();
                System.Console.WriteLine("done inserting");
                long insertMs = insertSw.ElapsedMilliseconds;
                var opSw = Stopwatch.StartNew();
                int txnAborts = MultiThreadedTransactions(tbl, txnManager, ratio);
                opSw.Stop();
                long opMs = opSw.ElapsedMilliseconds;
                stats.AddTransactionalResult((insertMs, opMs, insertAborts, txnAborts));
            }
        }
        stats.ShowAllStats();
        stats.SaveStatsToFile();
    }
}

public class BenchmarkStatistics {
    internal readonly List<long> insMsPerRun = new();
    internal readonly List<long> opsMsPerRun = new();
    internal readonly List<int> insAbortsPerRun = new();
    internal readonly List<int> txnAbortsPerRun = new();
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
        Console.WriteLine(GetInsDataString(operations, insMsPerRun));
        Console.WriteLine(GetOpsDataString(inserts, operations-inserts, opsMsPerRun));
        if (insAbortsPerRun.Count != 0){
            Console.WriteLine(GetInsAbortDataString(insAbortsPerRun));
            Console.WriteLine(GetTxnAbortDataString(txnAbortsPerRun));
        }
    }

    internal async void SaveStatsToFile(){
        string[] data = new string[]{
            GetInsDataString(operations, insMsPerRun),
            GetOpsDataString(inserts, operations-inserts, opsMsPerRun)
        }.Concat(insAbortsPerRun.Count != 0 ? new string[]{
            GetInsAbortDataString(insAbortsPerRun),
            GetTxnAbortDataString(txnAbortsPerRun)
        } : new string[0]).ToArray();
        string now = DateTime.Now.ToString("yyyyMMdd-HHmmss"); 
        await File.WriteAllLinesAsync($"{name}-{now}.txt", data);
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