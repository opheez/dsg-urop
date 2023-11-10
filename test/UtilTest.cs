using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace DB
{
    [TestClass]
    public unsafe class UtilTests
    {

        [TestMethod]
        public void TestLogEntryEquals(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Dictionary<int, Table> tables = new Dictionary<int, Table>();
            Table table = new Table(schema);
            tables.Add(table.GetHashCode(), table);

            KeyAttr keyAttr = new KeyAttr(12345, 67890, table);
            byte[] val = {8, 8, 8, 8};
            LogEntry entry = new LogEntry(4, 8, keyAttr, val);
            entry.lsn = 5;

            KeyAttr keyAttr2 = new KeyAttr(12345, 67890, table);
            byte[] val2 = {8, 8, 8, 8};
            LogEntry entry2 = new LogEntry(4, 8, keyAttr, val);
            entry2.lsn = 5;

            Assert.IsTrue(entry.Equals(entry2));
            entry2.val = new byte[]{8, 8, 8, 9};
            Assert.IsFalse(entry.Equals(entry2));
        }

        [TestMethod]
        public void TestLogEntrySerialize(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Dictionary<int, Table> tables = new Dictionary<int, Table>();
            Table table = new Table(schema);
            tables.Add(table.GetHashCode(), table);
            KeyAttr keyAttr = new KeyAttr(12345, 67890, table);
            byte[] val = {8, 8, 8, 8};
            LogEntry entry = new LogEntry(4, 8, keyAttr, val);
            entry.lsn = 5;

            Assert.IsTrue(entry.Equals(LogEntry.FromBytes(entry.ToBytes(), tables)));
        }


        private void PrintSpan(ReadOnlySpan<byte> val) {
            foreach(byte b in val){
                Console.Write(b);
            }
        }
    }
}