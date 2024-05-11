using FASTER.darq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace DB
{
    [TestClass]
    public unsafe class WriteAheadLogTests
    {
        public static int nCommitterThreads = 5;

        // Possible flows:
        // begin > write > finish 
        // prepare > recordok > begin > write > finish2pc

        // [TestMethod]
        // [ExpectedException(typeof(KeyNotFoundException))]
        // public void TestWriteFirst(){
        //     DarqWal darqWal = new DarqWal(new DarqId(0));
        //     PrimaryKey pk = new PrimaryKey(1, 1);
        //     darqWal.Write(1, ref pk, new TupleDesc[]{new TupleDesc(1, 1, 1)}, new byte[] { 1 });
        // }

        // [TestMethod]
        // [ExpectedException(typeof(KeyNotFoundException))]
        // public void TestFinis2pcFirst(){
        //     DarqWal darqWal = new DarqWal(new DarqId(0));
        //     darqWal.Finish2pc(1, LogType.Commit, new List<(long, long)>());
        // }

        // [TestMethod]
        // [ExpectedException(typeof(KeyNotFoundException))]
        // public void TestOkFirst(){
        //     DarqWal darqWal = new DarqWal(new DarqId(0));
        //     darqWal.RecordOk(1, 2);
        // }

        // TODO: mock DARQ capabilities

        // [TestMethod]
        // public void TestCorrectSingleNodeFlow(){
        //     DarqWal darqWal = new DarqWal(new DarqId(0));
        //     darqWal.Begin(1);
        //     darqWal.Write(1, new KeyAttr(1, 1, new Table(1, new (long, int)[] { (1, 1) })), new byte[] { 1 });
        //     darqWal.Finish(1, LogType.Commit);
        // }

        // [TestMethod]
        // public void TestCorrectShardedFlow(){
        //     DarqWal darqWal = new DarqWal(new DarqId(0));
        //     Table table = new Table(1, new (long, int)[] { (1, 1) });
        //     Dictionary<long, List<(KeyAttr, byte[])>> shardToWriteset = new Dictionary<long, List<(KeyAttr, byte[])>>
        //     {
        //         { 2, new List<(KeyAttr, byte[])> { (new KeyAttr(1, 1, table), new byte[] { 1 }) } }
        //     };
        //     darqWal.Prepare(shardToWriteset, 1);
        //     darqWal.RecordOk(1, 2);
        //     darqWal.Begin(1);
        //     darqWal.Write(1, new KeyAttr(1, 1, new Table(1, new (long, int)[] { (1, 1) })), new byte[] { 1 });
        //     darqWal.Finish(1, LogType.Commit);
        // }
    } 
}