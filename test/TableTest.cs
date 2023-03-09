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
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidKey(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            schema.Add(67890, (false, 32));

            Table test = new Table(schema);
            var retName = test.Read(11111, 12345);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestInvalidAttribute(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            schema.Add(67890, (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Upsert(11111, 12345, name);
            long attrAsLong = BitConverter.ToInt64(Encoding.ASCII.GetBytes("occupation"));
            var retName = test.Read(11111, attrAsLong);
        }

        [TestMethod]
        public void TestInsertRead(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            schema.Add(67890, (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Upsert(11111, 12345, name);
            test.Upsert(11111, 67890, BitConverter.GetBytes(21));
            var retName = test.Read(11111, 12345);
            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName).TrimEnd((Char)0));
            var retAge = test.Read(11111, 67890);
            Assert.AreEqual(21, BitConverter.ToInt64(retAge.ToArray()));
        }

        [TestMethod]
        public void TestMultipleInsertRead(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            schema.Add(67890, (false, 32));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("John Doe");
            test.Upsert(11111, 12345, name);
            test.Upsert(11111, 67890, BitConverter.GetBytes(21));
            test.Upsert(11111, 67890, BitConverter.GetBytes(40));
            name = Encoding.ASCII.GetBytes("Johnathan Doever");
            test.Upsert(11111, 12345, name);

            var retName = test.Read(11111, 12345);
            var retAge = test.Read(11111, 67890);

            Assert.AreEqual(Encoding.ASCII.GetString(name), Encoding.ASCII.GetString(retName).TrimEnd((Char)0));
            Assert.AreEqual(40, BitConverter.ToInt64(retAge.ToArray()));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestOversizeInsert(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("Jonathan Doever");
            test.Upsert(11111, 12345, name);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestEmptyInsert(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 10));

            Table test = new Table(schema);
            byte[] name = Encoding.ASCII.GetBytes("");
            test.Upsert(11111, 12345, name);
        }

        [TestMethod]
        public void TestVarLength(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (true,0));

            Table test = new Table(schema);
            byte[] input = Encoding.ASCII.GetBytes("123456789");
            var written = test.Upsert(11111, 12345, input);
            var y = test.Read(11111, 12345);
            CollectionAssert.AreEqual(input, y.ToArray());
            input = Encoding.ASCII.GetBytes("555555555");
            CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("123456789"), y.ToArray());
        }
    }
}