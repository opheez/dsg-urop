using System;
using System.Threading;
using System.Diagnostics;
public class TableBenchmark
{
    public static int PerThreadDataCount = 5000/2;
    private static int ThreadCount = 8/2;
    private static int DatasetSize = PerThreadDataCount * ThreadCount;
    private static int EntireSize = DatasetSize * 2; //TODO
    
    private static long[] keys;
    private static long[] attrs;
    private static byte[][] values;
    private static Dictionary<long,(bool,int)> schema;
    // private static Table tbl;
    private static Thread[] workers;
    private BenchmarkStatistics stats;
    private int IterationCount = 1;
    private double ratio;
    private int seed;
    public TableBenchmark(int seed, double ratio){
        schema = new Dictionary<long, (bool,int)>();
        workers = new Thread[ThreadCount];
        stats = new BenchmarkStatistics(DatasetSize, DatasetSize);
        this.ratio = ratio;
        this.seed = seed;

        // Load data
        keys = new long[EntireSize];
        attrs = new long[EntireSize];
        values = new byte[EntireSize][];
        Random r = new Random(seed);
        for (int i = 0; i < EntireSize; i++){
            keys[i] = r.NextInt64();
            attrs[i] = r.NextInt64();
            // // count how many reads/writes based on ratio and add to statistics
            // if (attrs[i] < Int64.MaxValue * ratio) {

            // } else {

            // }

            schema[attrs[i]] = (false,sizeof(long));
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
    }

    public void MultiThreadedUpserts(Table tbl)
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

    public void MultiThreadedUpsertsReads(Table tbl, double ratio)
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

    private void Upserts(Table tbl, int thread_idx){
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i + (PerThreadDataCount * thread_idx);
            tbl.Upsert(keys[loc],attrs[loc],values[loc]);
        }
    }
    private void UpsertsReads(Table tbl, int thread_idx, double ratio){
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i + (PerThreadDataCount * thread_idx);
            if (attrs[loc] < Int64.MaxValue * ratio) {
                tbl.Upsert(keys[loc],attrs[loc],values[loc]);
            } else {
                tbl.Read(keys[loc], attrs[loc]);
            }
        }
    }

    public void Run(){
        for (int i = 0; i < IterationCount; i++){
            Table tbl = new Table(schema);
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
        stats.ShowAllStats();
        stats.SaveStatsToFile();
        // return stats;
    }
}

public class BenchmarkStatistics {
    private readonly List<long> insMsPerRun = new();
    private readonly List<long> opsMsPerRun = new();
    private int inserts;
    private int operations;

    internal BenchmarkStatistics(int inserts, int operations)
    {
        this.inserts = inserts;
        this.operations = operations;
    }

    internal void AddResult((long ims, long oms) result)
    {
        insMsPerRun.Add(result.ims);
        opsMsPerRun.Add(result.oms);
    }

    internal void ShowAllStats(){
        Console.WriteLine(GetInsDataString(inserts, insMsPerRun));
        Console.WriteLine(GetOpsDataString(operations, opsMsPerRun));
    }

    internal async void SaveStatsToFile(){
        string[] data = {
            GetInsDataString(inserts, insMsPerRun),
            GetOpsDataString(operations, opsMsPerRun)
        };
        string now = DateTime.Now.ToString("yyyyMMdd-HHmmss"); 
        await File.WriteAllLinesAsync($"TableBenchMark-{now}.txt", data);
    }

    internal static string GetOpsDataString(int operations, List<long> opsMsPerRun) => $"{operations/opsMsPerRun.Average()} operations/ms ({operations} operations in {opsMsPerRun.Average()} ms)";
    internal static string GetInsDataString(int inserts, List<long> insMsPerRun) => $"{inserts/insMsPerRun.Average()} inserts/ms ({inserts} inserts in {insMsPerRun.Average()} ms)";

    // internal static string GetLoadingTimeLine(double insertsPerSec, long elapsedMs)
    //     => $"##00; {InsPerSec}: {insertsPerSec:N2}; sec: {(double)elapsedMs / 1000:N3}";

    // internal static string GetAddressesLine(AddressLineNum lineNum, long begin, long head, long rdonly, long tail)
    //     => $"##{(int)lineNum:00}; begin: {begin}; head: {head}; readonly: {rdonly}; tail: {tail}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string opsPerSecTag, double opsPerSec)
    //     => $"##{(int)lineNum:00}; {opsPerSecTag}: {opsPerSec:N2}; {OptionsString}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string meanTag, double mean, double stdev, double stdevpct)
    //     => $"##{(int)lineNum:00}; {meanTag}: {mean:N2}; stdev: {stdev:N1}; stdev%: {stdevpct:N1}; {OptionsString}";
}