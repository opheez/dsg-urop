using System;
public class VarLenTableBenchmark : TableBenchmark
{
    public VarLenTableBenchmark(int seed, double ratio) : base(seed, ratio) {
        System.Console.WriteLine("Var Len init");
        Random r = new Random(seed);
        // Load data
        for (int i = 0; i < AttrCount; i++){
            attrs[i] = r.NextInt64();
            schema[attrs[i]] = (true,-123);
        }

        int inserts = 0;
        for (int i = 0; i < DatasetSize; i++){
            keys[i] = r.NextInt64();
            if (keys[i] < Int64.MaxValue * ratio) {
                // count how many reads/writes based on ratio and add to statistics
                inserts++;
                keys[i+DatasetSize] = r.NextInt64(); // new key to write to
            }

            values[i] = new byte[keys[i]%100];
            r.NextBytes(values[i]);
        }

        stats = new BenchmarkStatistics("VarLenTableBenchmark", inserts, DatasetSize);
    }
}