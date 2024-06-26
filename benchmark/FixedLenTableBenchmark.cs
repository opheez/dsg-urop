using System;

namespace DB 
{
public class FixedLenTableBenchmark : TableBenchmark
{
    public FixedLenTableBenchmark(string name, BenchmarkConfig cfg, IWriteAheadLog? wal = null) : base(cfg) {
        System.Console.WriteLine("Init");
        this.wal = wal;
        int sizeOfAttr = 10;
        Random r = new Random(cfg.seed);
        // Load data
        int offset = 0;
        for (int i = 0; i < cfg.attrCount; i++){
            long attr = r.NextInt64();
            schema[i] = (attr, sizeOfAttr);
            td[i] = new TupleDesc(attr, sizeOfAttr, offset);
            offset += sizeOfAttr;
        }

        // randomly assign reads and writes
        int numWrites = (int)(cfg.datasetSize * cfg.ratio);
        for (int i = 0; i < numWrites; i++){
            long index = r.NextInt64(cfg.datasetSize-1);
            // if this index is already a write, find the next available index
            if (isWrite[(int)index]) {
                while (isWrite[(int)index]){
                    index += 1;
                }
            }
            isWrite[(int)index] = true;
        }

        for (int i = 0; i < cfg.datasetSize; i++){
            values[i] = new byte[sizeOfAttr*cfg.attrCount];
            r.NextBytes(values[i]);
        }
        stats = new BenchmarkStatistics($"{name}-FixedLenTableBenchmark", cfg, numWrites, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }

}
}