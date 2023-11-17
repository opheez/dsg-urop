using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace DB
{
    [TestClass]
    public unsafe class TransactionContextTests
    {

        [TestMethod]
        public void TestGetFromRset(){
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            KeyAttr ka1 = new KeyAttr(1, 1, tbl);
            byte[] val1 = new byte[]{1,2,3};
            ctx.AddReadSet(ka1, val1);
            byte[]? res1 = ctx.GetFromContext(ka1);
            
            KeyAttr ka2 = new KeyAttr(1, 2, tbl);
            byte[] val2 = new byte[]{9,8,7};
            ctx.AddReadSet(ka2, val2);
            ctx.AddReadSet(new KeyAttr(2, 1, tbl), new byte[]{5,5,5});
            byte[]? res2 = ctx.GetFromContext(ka1);
            byte[]? res3 = ctx.GetFromContext(ka2);

            CollectionAssert.AreEqual(val1, res1);
            CollectionAssert.AreEqual(val1, res2);
            CollectionAssert.AreEqual(val2, res3);
        }

        [TestMethod]
        public void TestGetFromWset(){
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            TupleDesc[] td = new TupleDesc[]{new TupleDesc(1, 3), new TupleDesc(2, 3)};
            byte[] val1 = new byte[]{1,2,3,4,5,6};
            KeyAttr ka1 = new KeyAttr(1, 1, tbl);
            KeyAttr ka2 = new KeyAttr(1, 2, tbl);
            ctx.AddWriteSet(tupleId, td, val1);
            byte[]? res1 = ctx.GetFromContext(ka1);
            byte[]? res2 = ctx.GetFromContext(ka2);
            
            TupleDesc[] td2 = new TupleDesc[]{new TupleDesc(2, 3)};
            byte[] val2 = new byte[]{9,8,7};
            ctx.AddWriteSet(tupleId, td2, val2);
            ctx.AddReadSet(new KeyAttr(2, 1, tbl), new byte[]{5,5,5});
            byte[]? res3 = ctx.GetFromContext(ka1);
            byte[]? res4 = ctx.GetFromContext(ka2);

            CollectionAssert.AreEqual(new byte[]{1,2,3}, res1);
            CollectionAssert.AreEqual(new byte[]{4,5,6}, res2);
            CollectionAssert.AreEqual(new byte[]{1,2,3}, res3);
            CollectionAssert.AreEqual(val2, res4);
        }

        [TestMethod]
        public void TestGetFromContextNull(){
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            TupleDesc[] td = new TupleDesc[]{new TupleDesc(2, 3)};
            byte[] val1 = new byte[]{4,5,6};
            ctx.AddWriteSet(tupleId, td, val1);
            byte[]? res1 = ctx.GetFromContext(new KeyAttr(1, 1, tbl));
            byte[]? res2 = ctx.GetFromContext(new KeyAttr(2, 1, tbl));
            byte[]? res3 = ctx.GetFromContext(new KeyAttr(1, 2, tbl));

            Assert.IsNull(res1);
            Assert.IsNull(res2);
            CollectionAssert.AreEqual(val1, res3);
        }
    }
}