using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
unsafe class Program {

    public static void Main(){
        Console.WriteLine("Hello, World!");
        // test.debug();
        // BenchmarkRunner.Run<TableBenchmarkDotNet>();
        TableBenchmark b = new FixedLenTableBenchmark(12345, 0.5);
        b.Run();
        b = new VarLenTableBenchmark(12345, 0.5);
        b.Run();
    }

}