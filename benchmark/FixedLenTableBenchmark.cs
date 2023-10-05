using System;

namespace DB 
{
public class FixedLenTableBenchmark : TableBenchmark
{
    public FixedLenTableBenchmark(string name, BenchmarkConfig cfg) : base(cfg) {
        Console.WriteLine("Fixed Len init");
        Random r = new Random(cfg.seed);
        // Load data
        for (int i = 0; i < cfg.attrCount; i++){
            long attr = r.NextInt64();
            attrs[i] = attr;
            schema[i] = (attr,sizeof(long));
        }

        // one out of every group will be a read, all others are writes
        int groupSize = (int)(1/(1-cfg.ratio));
        int numberOfGroups = (int) cfg.datasetSize/groupSize;
        int inserts = (groupSize - 1) * numberOfGroups;
        if (cfg.datasetSize % groupSize != 0) {
            int remaining = cfg.datasetSize - (groupSize * numberOfGroups);
            inserts += remaining - 1;
        }

        for (int i = 0; i < cfg.datasetSize; i++){
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics($"{name}-FixedLenTableBenchmark", cfg, inserts, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }

}
public class TransactionalFixedLenTableBenchmark : TableBenchmark
{
    public TransactionalFixedLenTableBenchmark(string name, BenchmarkConfig cfg) : base(cfg) {
        System.Console.WriteLine("Init");
        Random r = new Random(cfg.seed);
        // Load data
        for (int i = 0; i < cfg.attrCount; i++){
            long attr = r.NextInt64();
            attrs[i] = attr;
            schema[i] = (attr,sizeof(long));
        }

        // one out of every group will be a read, all others are writes
        int groupSize = (int)(1/(1-cfg.ratio));
        int numberOfGroups = (int) cfg.datasetSize/groupSize;
        int inserts = (groupSize - 1) * numberOfGroups;
        if (cfg.datasetSize % groupSize != 0) {
            int remaining = cfg.datasetSize - (groupSize * numberOfGroups);
            inserts += remaining - 1;
        }

        for (int i = 0; i < cfg.datasetSize; i++){
            values[i] = new byte[sizeof(long)];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics($"{name}-TransactionalFixedLenTableBenchmark", cfg, inserts, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }

}
}