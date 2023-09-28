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
            var retName = test.Read(11111, 12345);
            Assert.IsTrue(retName.IsEmpty);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidAttribute(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 8));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Insert(11111, 12345, name.AsSpan());
            long attrAsLong = BitConverter.ToInt64(Encoding.ASCII.GetBytes("occupation"));
            var retName = test.Read(11111, attrAsLong);
        }

        [TestMethod]
        public void TestInsertRead(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 8));
            schema.Add(67890, (false, 4));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Insert(11111, 12345, name.AsSpan());
            test.Insert(11111, 67890, BitConverter.GetBytes(21).AsSpan());
            var retName = test.Read(11111, 12345);
            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName));
            var retAge = test.Read(11111, 67890);
            Assert.AreEqual(21, BitConverter.ToInt32(retAge.ToArray()));
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
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Insert(11111, 12345, name.AsSpan());
            test.Insert(11111, 67890, BitConverter.GetBytes(21).AsSpan());
            test.Update(11111, 67890, BitConverter.GetBytes(40).AsSpan());
            name = Encoding.ASCII.GetBytes("Anna Lee");
            test.Update(11111, 12345, name.AsSpan());

            var retName = test.Read(11111, 12345);
            var retAge = test.Read(11111, 67890);

            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName));
            Assert.AreEqual(40, BitConverter.ToInt32(retAge.ToArray()));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestOversizeInsert(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("Jonathan Doever");
            test.Insert(11111, 12345, name.AsSpan());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestUndersizeInsert(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("a");
            test.Insert(11111, 12345, name.AsSpan());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestEmptyInsert(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("");
            test.Insert(11111, 12345, name.AsSpan());
        }

        [TestMethod]
        public void TestVarLength(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (true,0));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("123456789");
            test.Insert(11111, 12345, input.AsSpan());
            var y = test.Read(11111, 12345);
            CollectionAssert.AreEqual(input, y.ToArray());
            
            input = Encoding.ASCII.GetBytes("short");
            test.Insert(22222, 12345, input.AsSpan());
            var y1 = test.Read(11111, 12345);
            var y2 = test.Read(22222, 12345);

            CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("123456789"), y.ToArray());
            CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("123456789"), y1.ToArray());
            CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("short"), y2.ToArray());
        }
    }
}