using System;
public class FixedLenTableBenchmark : TableBenchmark
{
    public FixedLenTableBenchmark(int seed, double ratio) : base(seed, ratio) {
        System.Console.WriteLine("Fixed Len init");
        Random r = new Random(seed);
        // Load data
        for (int i = 0; i < AttrCount; i++){
            attrs[i] = r.NextInt64();
            schema[attrs[i]] = (false,sizeof(long));
        }

        int inserts = 0;
        for (int i = 0; i < DatasetSize; i++){
            keys[i] = r.NextInt64();
            if (keys[i] < Int64.MaxValue * ratio) {
                // count how many reads/writes based on ratio and add to statistics
                inserts++;
                keys[i+DatasetSize] = r.NextInt64(); // new key to write to
            }

            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics("FixedLenTableBenchmark", inserts, DatasetSize);

    }

}