using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace TableTests
{
    [TestClass]
    public unsafe class TableTests
    {


        [TestMethod]
        public void TestInsertRead(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("Ophelia");
            test.Set("key1", "name", name);
            test.Set("key1", "age", BitConverter.GetBytes(21));
            var retName = test.Get("key1", "name");
            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName).TrimEnd((Char)0));
            var retAge = test.Get("key1", "age");
            Assert.AreEqual(21, BitConverter.ToInt64(retAge.ToArray()));
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