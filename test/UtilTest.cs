using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace DB
{
    [TestClass]
    public unsafe class UtilTests
    {

        [TestMethod]
        public void TestPrimaryKeyEquals(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(1, schema);
            PrimaryKey tid1 = new PrimaryKey(table.GetId(), 12345);
            PrimaryKey tid2 = new PrimaryKey(table.GetId(), 12345);

            Assert.IsTrue(tid1.Equals(tid2));
        }

        [TestMethod]
        public void TestPrimaryKeySerialize(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(1, schema);
            PrimaryKey tid1 = new PrimaryKey(table.GetId(), 12345);
            PrimaryKey tid2 = PrimaryKey.FromBytes(tid1.ToBytes());
            Console.WriteLine(tid1);
            Console.WriteLine(tid2);

            Assert.IsTrue(tid1.Equals(tid2));
        }

        [TestMethod]
        public void TestKeyAttrEquals(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(1, schema);
            PrimaryKey pk = new PrimaryKey(table.GetId(), 12345);
            KeyAttr keyAttr1 = new KeyAttr(pk, 67890);
            KeyAttr keyAttr2 = new KeyAttr(pk, 67890);

            Assert.IsTrue(keyAttr1.Equals(keyAttr2));
        }

        [TestMethod]
        public void TestKeyAttrSerialize(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Dictionary<int, Table> tables = new Dictionary<int, Table>();
            Table table = new Table(1, schema);
            PrimaryKey pk = new PrimaryKey(table.GetId(), 12345);
            KeyAttr keyAttr = new KeyAttr(pk, 67890);
            byte[] bytes = keyAttr.ToBytes();
            KeyAttr keyAttr2 = KeyAttr.FromBytes(bytes);
            Console.WriteLine(keyAttr);
            
            Console.WriteLine(keyAttr2);

            Assert.IsTrue(keyAttr.Equals(keyAttr2));
        }

        [TestMethod]
        public void TestLogEntryEquals(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Dictionary<int, Table> tables = new Dictionary<int, Table>();
            Table table = new Table(1, schema);

            PrimaryKey pk = new PrimaryKey(table.GetId(), 12345);
            KeyAttr keyAttr = new KeyAttr(pk, 67890);
            byte[] val = {8, 8, 8, 8};
            LogEntry entry = new LogEntry(4, 8, keyAttr, val);
            entry.lsn = 5;

            KeyAttr keyAttr2 = new KeyAttr(pk, 67890);
            byte[] val2 = {8, 8, 8, 8};
            LogEntry entry2 = new LogEntry(4, 8, keyAttr2, val2);
            entry2.lsn = 5;

            Assert.IsTrue(entry.Equals(entry2));
            entry2.vals = new byte[][]{new byte[]{8, 8, 8, 9}};
            Assert.IsFalse(entry.Equals(entry2));
        }

        [TestMethod]
        public void TestLogEntrySerialize(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(1, schema);
            KeyAttr keyAttr = new KeyAttr(new PrimaryKey(table.GetId(), 12345), 67890);
            byte[] val = {8, 8, 8, 8};
            LogEntry entry = new LogEntry(4, 8, keyAttr, val);
            entry.lsn = 5;

            Assert.IsTrue(entry.Equals(LogEntry.FromBytes(entry.ToBytes())));
        }

        [TestMethod]
        public void TestLogEntrySerializePrepare(){
            (long,int)[] schema = {(12345,3), (56789, 4)};
            Table table = new Table(1, schema);
            KeyAttr keyAttr = new KeyAttr(new PrimaryKey(table.GetId(), 12345), 67890);
            KeyAttr keyAttr2 = new KeyAttr(new PrimaryKey(table.GetId(), 56789), 33333);
            byte[] val = {8, 8, 8};
            byte[] val2 = {5, 5, 5, 5};
            byte[] val3 = {4,4,4};

            LogEntry entry = new LogEntry(4, 8, new KeyAttr[]{keyAttr, keyAttr2, keyAttr}, new byte[][]{val, val2, val3});
            entry.lsn = 5;

            Assert.IsTrue(entry.Equals(LogEntry.FromBytes(entry.ToBytes())));
        }


        private void PrintSpan(ReadOnlySpan<byte> val) {
            foreach(byte b in val){
                Console.Write(b);
            }
            Console.WriteLine();
        }
    }
}