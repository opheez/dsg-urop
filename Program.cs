using System;
using System.Collections;
using System.Text;
using BenchmarkDotNet.Running;
unsafe class Program {

    public static void Main(){
        Console.WriteLine("Hello, World!");
        // test.debug();
        BenchmarkRunner.Run<TableBenchmark>();
    }

}