using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
public class TableBenchmark
{
    public int IterationCount = 500;
    public int ThreadCount = 10;
    private static long attr1 = 12345;
    private static Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>(){{attr1, (false, 64)}};
    private static readonly Table tbl = new Table(schema);

    [Benchmark]
    public void MultiThreadedUpsertsReads()
    {
        for (int thread = 0; thread < ThreadCount; thread++) {
            ThreadStart newThread = new ThreadStart()
        }

    }

    private void UpsertsReads(){
        byte[] bytesToInsert = BitConverter.GetBytes(500);
        for (int i = 0; i < IterationCount; i++){
            tbl.Upsert(i,attr1,bytesToInsert);
        }
        for (int i = 0; i < IterationCount; i++){
            tbl.Read(i,attr1);
        }
    }
}