using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace TableTests
{
    [TestClass]
    public unsafe class TableTests
    {

        [TestMethod]
        public void TestStorePointer(){
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

        [TestMethod]
        public void TestInsertRead(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("Ophelia");
            test.Set("a", "name", name);

            var y = test.Get("a", "name");
            System.Console.WriteLine(Encoding.ASCII.GetString(y));
        }

        [TestMethod]
        public void TestVarLength(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("attrName", (true,0));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("123456789");
            var written = test.Set("key1", "attrName", input);
            var y = test.Get("key1", "attrName");
            CollectionAssert.AreEqual(input, y.ToArray());
        }
    }
}