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
        // Hashtable x = new Hashtable();
		// int num = 7;
		// int* ptr = &num;
		// x.Add("age", ptr);
		// Console.WriteLine(typeof(int*));
        Dictionary<string,int> schema = new Dictionary<string, int>();
        schema.Add("name", 100);
        schema.Add("age", 32);

        Table test = new Table(schema);
        foreach (var x in test.catalog){
            Console.WriteLine(x);
        }
        byte[] name = Encoding.ASCII.GetBytes("Ophelia");
        test.Set("a", "name", name);

        var y = test.Get("a", "name");
        test.debug();

        // test.Initialize(schema);
        // Console.WriteLine(test.fields);
        // test.Insert("key1", )
    }
}