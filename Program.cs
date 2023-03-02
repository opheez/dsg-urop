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
        testVarLength();
        // test.debug();

    }

    public static void testStorePointer(){
        byte[] x = new byte[]{(byte)'a'};
		fixed (byte* ptr = &x[0]){
		    IntPtr addr = new IntPtr(ptr);
            byte[] memory = BitConverter.GetBytes(addr.ToInt64());
            System.Console.WriteLine(addr.ToInt64());
            System.Console.WriteLine(BitConverter.ToInt64(memory));
            byte* decodedPtr = (byte*)(new IntPtr(BitConverter.ToInt64(memory))).ToPointer();
            // byte* decodedPtr = (byte*)addr.ToPointer();
		    Console.WriteLine(*decodedPtr);
        }
    }

    public static void testInsertRead(){
        Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
        schema.Add("name", (false,100));
        schema.Add("age", (false, 32));

        Table test = new Table(schema);
        byte[] name = Encoding.ASCII.GetBytes("Ophelia");
        test.Set("a", "name", name);

        var y = test.Get("a", "name");
        System.Console.WriteLine(Encoding.ASCII.GetString(y));
    }

    public static void testVarLength(){
        Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
        schema.Add("uhoh", (true,0));

        Table test = new Table(schema);
        byte[] name = Encoding.ASCII.GetBytes("123456789");
        var written = test.Set("a", "uhoh", name);
        System.Console.WriteLine(BitConverter.ToInt64(written.ToArray()));
        var y = test.Get("a", "uhoh");
        System.Console.WriteLine(Encoding.ASCII.GetString(y));
    }
}