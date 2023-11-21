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
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            byte[] val = new byte[]{1,2,3};
            ctx.AddReadSet(tupleId, val);
        }

        [TestMethod]
        public void TestGetFromRset(){
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

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
            Table tbl = new Table(new (long,int)[]{(1,3), (2,3)});

            TransactionContext ctx = new TransactionContext();
            ctx.Init(0, 0);
            TupleId tupleId = new TupleId(1, tbl);
            TupleDesc[] td = new TupleDesc[]{new TupleDesc(1, 3), new TupleDesc(2, 3)};
            byte[] val1 = new byte[]{1,2,3,4,5,6};
            ctx.AddWriteSet(tupleId, td, val1);
            Dictionary<TupleDesc, byte[]> res1 = ctx.GetFromWriteset(tupleId);
            
            Dictionary<TupleDesc, byte[]> expected1 = new Dictionary<TupleDesc, byte[]>(){{td[0], new byte[]{1,2,3}}, {td[1], new byte[]{4,5,6}}};
            Assert.IsTrue(isDictEqual(expected1, res1));

            TupleDesc[] td2 = new TupleDesc[]{new TupleDesc(2, 3)};
            byte[] val2 = new byte[]{9,8,7};
            ctx.AddWriteSet(tupleId, td2, val2);
            Dictionary<TupleDesc, byte[]> res2 = ctx.GetFromWriteset(tupleId);

            Dictionary<TupleDesc, byte[]> expected2 = new Dictionary<TupleDesc, byte[]>(){{td[0], new byte[]{1,2,3}}, {td[1], new byte[]{9,8,7}}};
            Assert.IsTrue(isDictEqual(expected2, res2));
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
            ReadOnlySpan<byte> res1 = ctx.GetFromReadset(new TupleId(1, tbl));
            Dictionary<TupleDesc, byte[]> res2 = ctx.GetFromWriteset(new TupleId(2, tbl));

            Assert.IsTrue(res1 == null);
            Assert.IsTrue(res2 == null);
        }

        private bool isDictEqual(Dictionary<TupleDesc, byte[]> dict1, Dictionary<TupleDesc, byte[]> dict2){
            if (dict1.Count != dict2.Count){
                return false;
            }
            foreach (var item in dict1){
                byte[] val1 = item.Value;
                if (!dict2.ContainsKey(item.Key) || !MemoryExtensions.SequenceEqual(val1.AsSpan(), dict2[item.Key].AsSpan())){
                    return false;
                }
            }
            return true;
        }
    } 
}