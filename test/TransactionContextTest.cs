using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace DB
{
    [TestClass]
    public unsafe class TransactionContextTests
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidReadsetAdd(){
            Table tbl = new Table(1, new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            byte[] val = new byte[]{1,2,3};
            ctx.AddReadSet(tupleId, val);
        }

        [TestMethod]
        public void TestGetFromRset(){
            Table tbl = new Table(1, new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            byte[] val1 = new byte[]{1,2,3,4,5,6};
            ctx.AddReadSet(tupleId, val1);
            ReadOnlySpan<byte> res1 = ctx.GetFromReadset(tupleId);
            
            byte[] val2 = new byte[]{1,2,3,9,8,7};
            ctx.AddReadSet(tupleId, val2);
            // should have no effect
            ctx.AddReadSet(new TupleId(2, tbl), new byte[]{5,5,5,5,5,5});
            ReadOnlySpan<byte> res2 = ctx.GetFromReadset(tupleId);

            Assert.IsTrue(MemoryExtensions.SequenceEqual(res1, val1));
            Assert.IsTrue(MemoryExtensions.SequenceEqual(res2, val2));

        }

        [TestMethod]
        public void TestGetFromWset(){
            Table tbl = new Table(1, new (long,int)[]{(1,3), (2,3), (3,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            TupleDesc[] td = new TupleDesc[]{new TupleDesc(1, 3, 0), new TupleDesc(2, 3, 3)};
            byte[] val1 = new byte[]{1,2,3,4,5,6};
            ctx.AddWriteSet(tupleId, td, val1);
            (TupleDesc[], byte[]) res1 = ctx.GetFromWriteset(tupleId);
            

            TupleDesc[] td2 = new TupleDesc[]{new TupleDesc(2, 3, 0)};
            byte[] val2 = new byte[]{9,8,7};
            ctx.AddWriteSet(tupleId, td2, val2);
            (TupleDesc[], byte[]) res2 = ctx.GetFromWriteset(tupleId);

            TupleDesc[] td3 = new TupleDesc[]{new TupleDesc(3, 3, 0)};
            byte[] val3 = new byte[]{5,5,5};
            ctx.AddWriteSet(tupleId, td3, val3);
            (TupleDesc[], byte[]) res3 = ctx.GetFromWriteset(tupleId);

            CollectionAssert.AreEqual(val1, res1.Item2);
            CollectionAssert.AreEqual(td, res1.Item1);
            CollectionAssert.AreEqual(new byte[]{1,2,3,9,8,7}, res2.Item2);
            CollectionAssert.AreEqual(new byte[]{1,2,3,9,8,7,5,5,5}, res3.Item2);
            CollectionAssert.AreEqual(td, res2.Item1);
        }

        [TestMethod]
        public void TestGetFromContextNull(){
            Table tbl = new Table(1, new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            TupleDesc[] td = new TupleDesc[]{new TupleDesc(2, 3, 3)};
            byte[] val1 = new byte[]{4,5,6};
            ctx.AddWriteSet(tupleId, td, val1);
            ReadOnlySpan<byte> res1 = ctx.GetFromReadset(new TupleId(1, tbl));
            (TupleDesc[], byte[]) res2 = ctx.GetFromWriteset(new TupleId(2, tbl));

            Assert.IsTrue(res1 == null);
            Assert.IsTrue(res2.Item1 == null);
            Assert.IsTrue(res2.Item2 == null);
        }

    } 
}