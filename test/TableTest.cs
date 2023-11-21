using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Jobs;

namespace DB
{
    [TestClass]
    public unsafe class TableTests
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidSizeSchema(){
            (long,int)[] schema = {(12345,0)};
        
            Table test = new Table(schema);
        }

        [TestMethod]
        public void TestValidVarLenSchema(){
            (long,int)[] schema = {(12345,-1)};

            Table test = new Table(schema);
        }

        [TestMethod]
        public void TestInvalidKey(){
            (long,int)[] schema = {(12345,100)};
            Table test = new Table(schema);

            TupleId ka = new TupleId(11111, test);
            var retName = test.Read(ka);
            Assert.IsTrue(Util.IsEmpty(retName));
        }

        // [TestMethod]
        // [ExpectedException(typeof(KeyNotFoundException))]
        // public void TestInvalidAttribute(){
        //     (long,int)[] schema = {(12345,8)};
        //     Table test = new Table(schema);

        //     byte[] name = Encoding.ASCII.GetBytes("John Doe");
        //     KeyAttr ka = new KeyAttr(11111, 12345, test);
        //     test.Write(ka, name);
        //     long attrAsLong = BitConverter.ToInt64(Encoding.ASCII.GetBytes("occupation"));
        //     var retName = test.Read(new TupleId(11111, test));
        // }

        [TestMethod]
        public void TestWriteRead(){
            (long,int)[] schema = {(12345,8), (67890, 4)};
            Table test = new Table(schema);

            byte[] input = Encoding.ASCII.GetBytes("John Doe");
            KeyAttr ka = new KeyAttr(11111, 12345, test);
            test.Write(ka, input);
            var ret = test.Read(new TupleId(ka.Key, ka.Table));
            CollectionAssert.AreEqual(input.Concat(new byte[4]).ToArray(), ret.ToArray());
        }

        [TestMethod]
        public void TestMultipleWriteRead(){
            (long,int)[] schema = {(12345,8), (67890, 4)};
            Table test = new Table(schema);
            
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            KeyAttr ka = new KeyAttr(11111, 12345, test);
            KeyAttr ka2 = new KeyAttr(11111, 67890, test);
            byte[] val1 = BitConverter.GetBytes(21);
            byte[] val2 = BitConverter.GetBytes(40);
            test.Write(ka, name);
            test.Write(ka2, val1);
            test.Write(ka2, val2);
            name = Encoding.ASCII.GetBytes("Anna Lee");
            test.Write(ka, name);

            var retName = test.Read(new TupleId(ka.Key, ka.Table));

            Assert.AreEqual(Encoding.ASCII.GetString(name.Concat(val2).ToArray()), Encoding.ASCII.GetString(retName));
        }

        // [TestMethod]
        // public void TestVarLength(){
        //     (long,int)[] schema = {(12345,-1)};
        //     Table test = new Table(schema);
            
        //     byte[] input = Encoding.ASCII.GetBytes("123456789");
        //     KeyAttr ka = new KeyAttr(11111, 12345, test);
        //     test.Write(ka, input);
        //     var y = test.Read(new TupleId(ka.Key, ka.Table));
        //     CollectionAssert.AreEqual(input, y.ToArray());
            
        //     byte[] input2 = Encoding.ASCII.GetBytes("short");
        //     KeyAttr ka2 = new KeyAttr(22222, 12345, test);
        //     test.Write(ka2, input2);
        //     var y1 = test.Read(new TupleId(ka.Key, ka.Table));
        //     var y2 = test.Read(new TupleId(ka2.Key, ka2.Table));

        //     CollectionAssert.AreEqual(input, y1.ToArray());
        //     CollectionAssert.AreEqual(input2, y2.ToArray());
        // }

        private void PrintSpan(ReadOnlySpan<byte> val) {
            foreach(byte b in val){
                Console.Write(b);
            }
        }
    }
}