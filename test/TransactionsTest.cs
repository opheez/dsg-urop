using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using System.Runtime.InteropServices;

namespace DB
{
    /// <summary>
    /// For Ti that comes before Tj
    /// </summary>
    [TestClass]
    public unsafe class TrasactionTests
    {
        [TestMethod]
        public void TestSerial(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            var v3 = table.Upsert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v1 = table.Read(new KeyAttr(1,12345, table), t);
            var success = txnManager.Commit(t);
            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(v1.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), W(Ti) intersects W(Tj)
        /// </summary>
        public void TestWRNoIntersectWWIntersect(){

        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), R(Ti) intersects W(Tj)
        /// </summary>
        public void TestWRNoIntersectRWIntersect(){

        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), R(Ti) intersects W(Tj),  W(Ti) does not intersects W(Tj)
        /// </summary>
        public void TestWRNoIntersectRWIntersectWWNoIntersect(){

        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect R(Tj), Ti doesnt finish writing before Tj starts
        /// </summary>
        public void TestWRIntersectRWIntersectWWNoIntersect(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false,100));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            var v1 = table.Upsert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v2 = table.Read(new KeyAttr(1,12345, table), t);
            TransactionContext t2 = txnManager.Begin();
            var v3 = table.Read(new KeyAttr(1,12345, table), t2);
            var v4 = table.Upsert(new KeyAttr(1,12345, table), BitConverter.GetBytes(5).AsSpan(), t2);
            var v5 = table.Read(new KeyAttr(1,12345, table), t2);
            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);
            Assert.IsTrue(v3.IsEmpty, "New context should not read uncommitted value");
            Assert.IsTrue(success, "Transaction was unable to commit");
            Assert.IsFalse(success2, "Transaction 2 should abort");
            CollectionAssert.AreEqual(v1.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect (W(Tj) U R(Tj)), Tj overlaps with Ti validation or write phase
        /// </summary>
        public void TestWRUnionWIntersect(){
            // must create separate threads for each txn context so that committing doesn't block
            // and other context can commence whilst validation occurs, nondeterminstic though!
            bool success = false;

            Assert.IsFalse(success);
        }

    }
}