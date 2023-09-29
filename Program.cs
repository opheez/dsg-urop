using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
using DB;
unsafe class Program {

    public static void Main(){
        Console.WriteLine("Hello, World!");
        TableBenchmark b = new TransactionalFixedLenTableBenchmark(12345, 0.5);
        b.RunTransactions();
        // b = new VarLenTableBenchmark(12345, 0.5);
        // b.Run();

    }

}