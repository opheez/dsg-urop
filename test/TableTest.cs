using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace TableTests
{
    [TestClass]
    public unsafe class TableTests
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidSizeSchema(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false, 0));

            Table test = new Table(schema);
        }

        [TestMethod]
        public void TestValidVarLenSchema(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (true, -1));

            Table test = new Table(schema);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidKey(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            var retName = test.Get("key1", "name");
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidAttribute(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Set("key1", "name", name);
            var retName = test.Get("key1", "occupation");
        }

        [TestMethod]
        public void TestInsertRead(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Set("key1", "name", name);
            test.Set("key1", "age", BitConverter.GetBytes(21));
            var retName = test.Get("key1", "name");
            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName).TrimEnd((Char)0));
            var retAge = test.Get("key1", "age");
            Assert.AreEqual(21, BitConverter.ToInt64(retAge.ToArray()));
        }

        [TestMethod]
        public void TestMultipleInsertRead(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false,100));
            schema.Add("age", (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Set("key1", "name", name);
            test.Set("key1", "age", BitConverter.GetBytes(21));
            test.Set("key1", "age", BitConverter.GetBytes(40));
            name = Encoding.ASCII.GetBytes("Johnathan Doever");
            test.Set("key1", "name", name);

            var retName = test.Get("key1", "name");
            var retAge = test.Get("key1", "age");

            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName).TrimEnd((Char)0));
            Assert.AreEqual(40, BitConverter.ToInt64(retAge.ToArray()));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestOversizeInsert(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("Jonathan Doever");
            test.Set("key1", "name", name);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestEmptyInsert(){
            Dictionary<string,(bool,int)> schema = new Dictionary<string, (bool,int)>();
            schema.Add("name", (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("");
            test.Set("key1", "name", name);
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
            input = Encoding.ASCII.GetBytes("555555555");
            CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("123456789"), y.ToArray());
        }
    }
}