using System;

namespace DB 
{
public class FixedLenTableBenchmark : TableBenchmark
{
    public FixedLenTableBenchmark(int seed, double ratio) : base(seed, ratio) {
        Console.WriteLine("Fixed Len init");
        Random r = new Random(seed);
        // Load data
        for (int i = 0; i < AttrCount; i++){
            long attr = r.NextInt64();
            attrs[i] = attr;
            schema[i] = (attr,sizeof(long));
        }

        // one out of every group will be a read, all others are writes
        int groupSize = (int)(1/(1-ratio));
        int numberOfGroups = (int) DatasetSize/groupSize;
        int inserts = (groupSize - 1) * numberOfGroups;
        if (DatasetSize % groupSize != 0) {
            int remaining = DatasetSize - (groupSize * numberOfGroups);
            inserts += remaining - 1;
        }

        for (int i = 0; i < DatasetSize; i++){
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics("FixedLenTableBenchmark", inserts, DatasetSize);
        System.Console.WriteLine("Done init");
    }

}
public class TransactionalFixedLenTableBenchmark : TableBenchmark
{
    public TransactionalFixedLenTableBenchmark(int seed, double ratio) : base(seed, ratio) {
        System.Console.WriteLine("Init");
        Random r = new Random(seed);
        // Load data
        for (int i = 0; i < AttrCount; i++){
            long attr = r.NextInt64();
            attrs[i] = attr;
            schema[i] = (attr,sizeof(long));
        }

        // one out of every group will be a read, all others are writes
        int groupSize = (int)(1/(1-ratio));
        int numberOfGroups = (int) DatasetSize/groupSize;
        int inserts = (groupSize - 1) * numberOfGroups;
        if (DatasetSize % groupSize != 0) {
            int remaining = DatasetSize - (groupSize * numberOfGroups);
            inserts += remaining - 1;
        }

        for (int i = 0; i < DatasetSize; i++){
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics("TransactionalFixedLenTableBenchmark", inserts, DatasetSize);
        System.Console.WriteLine("Done init");
    }

}
}