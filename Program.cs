using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
using DB;
unsafe class Program {

    public static void Main(){
        Console.WriteLine("Hello, World!");
        // test.debug();
        // BenchmarkRunner.Run<TableBenchmarkDotNet>();
        // TableBenchmark b = new FixedLenTableBenchmark(12345, 0.5);
        // b.Run();
        // b = new VarLenTableBenchmark(12345, 0.5);
        // b.Run();
        Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
        schema.Add(12345, (false,100));
        schema.Add(67890, (false, 32));

        Table table = new Table(schema);
        TransactionContext t = TransactionManager.Begin();
        var v3 = table.Upsert(new KeyAttr(1,12345), BitConverter.GetBytes(21).AsSpan(), t);
        var v1 = table.Read(new KeyAttr(1,12345), t);
        // var v2 = table.Read(new KeyAttr(2,12345), t);
        var success = TransactionManager.Commit(t);
        if (success) {
        // send v3 back to client, or some other logic from the upper level execution engine
        }

    }

}