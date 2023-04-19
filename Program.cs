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

        Table test = new Table(schema);
        
        

    }

}