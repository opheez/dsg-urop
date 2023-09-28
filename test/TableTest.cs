using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using System.Runtime.InteropServices;

namespace DB
{
    [TestClass]
    public unsafe class TableTests
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidSizeSchema(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 0));

            Table test = new Table(schema);
        }

        [TestMethod]
        public void TestValidVarLenSchema(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (true, -1));

            Table test = new Table(schema);
        }

        [TestMethod]
        public void TestInvalidKey(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));

            Table test = new Table(schema);
            TupleDesc[] td = {new TupleDesc(12345, 100)};
            var retName = test.Read(11111, td);
            Assert.IsTrue(retName.IsEmpty);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidAttribute(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 8));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            TupleDesc[] td = {new TupleDesc(12345, 8)};
            test.Insert(11111, td, name.AsSpan());
            long attrAsLong = BitConverter.ToInt64(Encoding.ASCII.GetBytes("occupation"));
            TupleDesc[] td2 = {new TupleDesc(attrAsLong, 8)};
            var retName = test.Read(11111, td2);
        }

        [TestMethod]
        public void TestInsertRead(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 8));
            schema.Add(67890, (false, 4));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("John Doe");
            TupleDesc[] td = {new TupleDesc(12345, 8)};
            test.Insert(11111, td, input);
            var ret = test.Read(11111, td);
            CollectionAssert.AreEqual(input, ret.ToArray());
        }

        // [TestMethod]
        // public void TestInsertReadRecord(){
        //     Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
        //     schema.Add(12345, (false, 8));
        //     schema.Add(67890, (false, 4));

        //     Table test = new Table(schema);
        //     byte[] name = Encoding.ASCII.GetBytes("John Doe");
        //     Span<byte> record = new byte[test.rowSize]; //todo: accesing rowSize internal var
        //     name.AsSpan().CopyTo(record);
        //     BitConverter.GetBytes(21).AsSpan().CopyTo(record.Slice(8, 4));
        //     test.Insert(11111, record);
        //     var retName = test.Read(11111);
        //     Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName.Slice(0, 8)));
        //     Assert.AreEqual(21, BitConverter.ToInt32(retName.Slice(8, 4).ToArray()));
        // }

        [TestMethod]
        public void TestMultipleInsertRead(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 8));
            schema.Add(67890, (false, 4));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("John Doe").Concat(BitConverter.GetBytes(21)).ToArray();
            TupleDesc[] td = {new TupleDesc(12345, 8), new TupleDesc(67890, 4)};
            test.Insert(11111, td, input);

            byte[] input2 = Encoding.ASCII.GetBytes("Anna Lee").Concat(BitConverter.GetBytes(40)).ToArray();
            TupleDesc[] td2 = {new TupleDesc(12345, 8), new TupleDesc(67890, 4)};
            test.Update(11111, td2, input2);

            var ret = test.Read(11111, td2);
            CollectionAssert.AreEqual(input2, ret.ToArray());
        }

        // [TestMethod]
        // [ExpectedException(typeof(ArgumentException))]
        // public void TestOversizeInsert(){
        //     Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
        //     schema.Add(12345, (false, 10));

        //     Table test = new Table(schema);
        //     TupleDesc[] td = {new TupleDesc(12345, 10)};
        //     byte[] name = Encoding.ASCII.GetBytes("Jonathan Doever");
        //     test.Insert(11111, td, name);
        // }

        // [TestMethod]
        // [ExpectedException(typeof(ArgumentOutOfRangeException))]
        // public void TestUndersizeInsert(){
        //     Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
        //     schema.Add(12345, (false, 10));

        //     Table test = new Table(schema);
        //     TupleDesc[] td = {new TupleDesc(12345, 10)};
        //     byte[] name = Encoding.ASCII.GetBytes("a");
        //     test.Insert(11111, td, name);
        // }

        // [TestMethod]
        // [ExpectedException(typeof(ArgumentException))]
        // public void TestEmptyInsert(){
        //     Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
        //     schema.Add(12345, (false, 10));

        //     Table test = new Table(schema);
        //     TupleDesc[] td = {new TupleDesc(12345, 10)};
        //     byte[] name = Encoding.ASCII.GetBytes("");
        //     test.Insert(11111, td, name);
        // }

        [TestMethod]
        public void TestVarLength(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (true,0));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("123456789");
            TupleDesc[] td = {new TupleDesc(12345, input.Length)};
            test.Insert(11111, td, input);
            var y = test.Read(11111, td);
            CollectionAssert.AreEqual(input, y.ToArray());
            
            byte[] input2 = Encoding.ASCII.GetBytes("short");
            TupleDesc[] td2 = {new TupleDesc(12345, input2.Length)};
            test.Insert(22222, td2, input2);
            var y1 = test.Read(11111, td);
            var y2 = test.Read(22222, td2);

            CollectionAssert.AreEqual(input, y1.ToArray());
            CollectionAssert.AreEqual(input2, y2.ToArray());
        }

        private void PrintSpan(ReadOnlySpan<byte> val) {
            foreach(byte b in val){
                Console.Write(b);
            }
        }
    }
}