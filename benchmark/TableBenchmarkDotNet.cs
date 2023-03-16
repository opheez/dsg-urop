using System;
using System.Threading;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
[ThreadingDiagnoser]
public class TableBenchmarkDotNet
{
    public static int PerThreadDataCount = 5000/2;
    private static int ThreadCount = 8/2;
    private static int DatasetSize = PerThreadDataCount * ThreadCount;
    private static int EntireSize = DatasetSize * 2; //TODO
    
    private static int seed = 12345;
    private static long[] keys;
    private static long[] attrs;
    private static byte[][] values;
    private static Dictionary<long,(bool,int)> schema;
    private static Table tbl;
    private static Thread[] workers;

    public TableBenchmarkDotNet(){
        schema = new Dictionary<long, (bool,int)>();
        workers = new Thread[ThreadCount];

        // Load data
        keys = new long[EntireSize];
        attrs = new long[EntireSize];
        values = new byte[EntireSize][];
        Random r = new Random(seed);
        for (int i = 0; i < EntireSize; i++){
            keys[i] = r.NextInt64();
            attrs[i] = r.NextInt64();
            schema[attrs[i]] = (false,sizeof(long));
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }

        tbl = new Table(schema);
        Setup();
    }

    public void Setup()
    {
        for (int i = 0; i < DatasetSize; i++){
            tbl.Upsert(keys[i],attrs[i],values[i]);
        }
    }

    [Benchmark]
    public void MultiThreadedUpserts()
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread] = new Thread(() => Upserts(thread));
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
    }

    [Benchmark]
    [Arguments(0.2)]
    [Arguments(0.5)]
    [Arguments(0.8)]
    public void MultiThreadedUpsertsReads(double ratio)
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread] = new Thread(() => UpsertsReads(thread, ratio));
            workers[thread].Start();
        }
        for (int thread = 0; thread < ThreadCount; thread++) {
            workers[thread].Join();
        }
    }

    [Benchmark]
    [Arguments(0.2)]
    [Arguments(0.5)]
    [Arguments(0.8)]
    public void SingleThreadUpsertsReads(double ratio)
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            UpsertsReads(thread, ratio);
        }
    }

    // Our data layout can be visualized as 
    // |————-Read/Write-——||———-----Write—————|
    // |—Thread—||—Thread—||—Thread—||—Thread—|
    // Where each thread is PerThreadDataCount long.
    private void Upserts(int thread_idx){
        // Write the second half of our data
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i * thread_idx + DatasetSize;
            tbl.Upsert(keys[loc],attrs[loc],values[loc]);
        }
    }
    private void UpsertsReads(int thread_idx, double ratio){
        // Write the second half of our data
        for (int i = 0; i < PerThreadDataCount; i++){
            int loc = i * thread_idx;
            if (attrs[loc] < Int64.MaxValue * ratio) {
                tbl.Upsert(keys[loc],attrs[loc],values[loc]);
            } else {
                tbl.Read(keys[loc], attrs[loc]);
            }
        }
    }
}