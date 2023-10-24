using System;

namespace DB {
public struct StoredProcedure {
    internal string name;
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
        BenchmarkConfig ycsbCfg = new BenchmarkConfig(
            ratio: writeRatio,
            seed: seed,
            attrCount: 10,
            threadCount: 12,
            iterationCount: 3,
            logWal: logWal
        );
        TableBenchmark b = new TransactionalFixedLenTableBenchmark("SP", ycsbCfg);
        b.RunTransactions();
    }

    public static StoredProcedure GetWorkloadAUpdateHeavy(LogWAL logWal){
        return new StoredProcedure("Workload_A", 12345, 0.5, logWal);
    }
    public static StoredProcedure GetWorkload8020UpdateHeavy(LogWAL logWal){
        return new StoredProcedure("Workload_8020", 12345, 0.2, logWal);
    }


}
}