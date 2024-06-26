using System;

namespace DB {
public struct StoredProcedure {
    internal string name;
    internal IWriteAheadLog? wal;
    internal int seed;
    internal double writeRatio;

    public StoredProcedure(string name, int seed, double writeRatio, IWriteAheadLog? wal = null){
        this.name = name;
        this.seed = seed;
        this.writeRatio = writeRatio;
        this.wal = wal;
    }

    public void Run(){
        BenchmarkConfig testCfg = new BenchmarkConfig(
            seed: seed,
            ratio: writeRatio,
            attrCount: 10,
            threadCount: 4,
            perThreadDataCount: 10,
            iterationCount: 1
        );
        // BenchmarkConfig ycsbCfg = new BenchmarkConfig(
        //     seed: seed,
        //     ratio: writeRatio,
        //     attrCount: 10,
        //     threadCount: 12,
        //     iterationCount: 3
        // );
        TableBenchmark b = new FixedLenTableBenchmark(name, testCfg, wal);
        b.RunTransactions();
    }

    public static StoredProcedure GetWorkloadAUpdateHeavy(IWriteAheadLog wal){
        return new StoredProcedure("Workload_A", 12345, 0.5, wal);
    }
    public static StoredProcedure GetWorkload8020UpdateHeavy(IWriteAheadLog wal){
        return new StoredProcedure("Workload_8020", 12345, 0.2, wal);
    }


}
}