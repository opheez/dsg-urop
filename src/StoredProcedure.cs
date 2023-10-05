using System;

namespace DB {
public class StoredProcedure {
    internal string name;
    internal int IterationCount;
    public int PerThreadDataCount = 100000; // enough to be larger than l3 cache
    internal int ThreadCount = 16;
    internal int AttrCount = 2;
    internal LogWAL logWal;
    internal int seed;
    internal double writeRatio;

    public StoredProcedure(string name, int seed, double writeRatio, LogWAL logWal){
        this.name = name;
        this.seed = seed;
        this.writeRatio = writeRatio;
        this.logWal = logWal;
    }

    public void Run(){
        TableBenchmark b = new TransactionalFixedLenTableBenchmark(seed, writeRatio);
        b.WithLogWAL(logWal);
        b.RunTransactions();
    }

    public static StoredProcedure GetWorkloadAUpdateHeavy(LogWAL logWal){
        return new StoredProcedure("Workload_A", 12345, 0.5, logWal);
    }


}
}