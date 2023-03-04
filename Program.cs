using System;
using System.Collections;
using System.Text;
unsafe class Program {
    struct TableRow {
        internal char name;
        internal int age;
    }

    public static void Main(){

        // See https://aka.ms/new-console-template for more information
        Console.WriteLine("Hello, World!");
        // test.debug();
        byte[] test = new byte[100];
        var span = new Span<byte>(test,50,10);

    }

}