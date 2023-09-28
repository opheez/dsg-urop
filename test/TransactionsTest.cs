using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;

namespace DB
{
    /// <summary>
    /// For Ti that comes before Tj
    /// </summary>
    [TestClass]
    public unsafe class TrasactionTests
    {
        private bool Commit(TransactionManager txnManager, TransactionContext t){
            var success = txnManager.Commit(t);
            return success;
        }

        [TestMethod]
        public void TestNoAttribute(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 4));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            table.Insert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v1 = table.Read(new KeyAttr(1,12345, table), t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(v1.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        public void TestSerial(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 4));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            table.Insert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v1 = table.Read(new KeyAttr(1,12345, table), t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(v1.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), W(Ti) intersects W(Tj) (Key 1), R(Ti) intersects W(Tj) (Key 1)
        /// </summary>
        public void TestWRNoIntersectWWIntersectWRIntersect(){
            System.Console.WriteLine("here");
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 4));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();
            System.Console.WriteLine("agafd");
            TransactionContext t = txnManager.Begin();
            table.Insert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v2 = table.Read(new KeyAttr(1,12345, table), t);

            TransactionContext t2 = txnManager.Begin();
            var v5 = table.Read(new KeyAttr(2,12345, table), t2);
            table.Update(new KeyAttr(1,12345, table), BitConverter.GetBytes(5).AsSpan(), t2);
            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);


            TransactionContext t3 = txnManager.Begin();
            var v6 = table.Read(new KeyAttr(1,12345, table), t3);
            var success3 = txnManager.Commit(t3);
            txnManager.Terminate();

            Assert.IsTrue(success2, "Transaction 2 was unable to commit");
            Assert.IsTrue(success3, "Transaction 3 was unable to commit");
            CollectionAssert.AreEqual(v2.ToArray().AsSpan().ToArray(), BitConverter.GetBytes(21));
            CollectionAssert.AreEqual(v5.ToArray().AsSpan().ToArray(), Array.Empty<byte>());
            CollectionAssert.AreEqual(v6.ToArray().AsSpan().ToArray(), BitConverter.GetBytes(5));
        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), R(Ti) intersects W(Tj), W(Ti) does not intersects W(Tj)
        /// </summary>
        public void TestWRNoIntersectRWIntersectWWNoIntersect(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 4));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            table.Insert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v2 = table.Read(new KeyAttr(2,12345, table), t);
            // Thread thread = new Thread(() => Commit(txnManager, t)); 

            TransactionContext t2 = txnManager.Begin();
            var v5 = table.Read(new KeyAttr(2,12345, table), t2);
            table.Insert(new KeyAttr(2,12345, table), BitConverter.GetBytes(5).AsSpan(), t2);

            // thread.Start();
            // while (t.status == TransactionStatus.Idle){} // make sure Ti completed read phase
            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);

            TransactionContext t3 = txnManager.Begin();
            var v6 = table.Read(new KeyAttr(1,12345, table), t3);
            var v7 = table.Read(new KeyAttr(2,12345, table), t3);
            var success3 = txnManager.Commit(t3);
            txnManager.Terminate();

            Assert.IsTrue(success2, "Transaction 2 was unable to commit");
            Assert.IsTrue(success3, "Transaction 3 was unable to commit");
            CollectionAssert.AreEqual(v2.ToArray().AsSpan().ToArray(), Array.Empty<byte>());
            CollectionAssert.AreEqual(v5.ToArray().AsSpan().ToArray(), Array.Empty<byte>());
            // System.Console.WriteLine($"val: {v6.ToArray().Length}");
            CollectionAssert.AreEqual(v6.ToArray().AsSpan().ToArray(), BitConverter.GetBytes(21));
            CollectionAssert.AreEqual(v7.ToArray().AsSpan().ToArray(), BitConverter.GetBytes(5));
        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect R(Tj), Ti doesnt finish writing before Tj starts
        /// </summary>
        public void TestWRIntersectRWIntersectWWNoIntersect(){
            Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            schema.Add(12345, (false, 4));
            schema.Add(56789, (false, 4));
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager();
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            table.Insert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            var v2 = table.Read(new KeyAttr(1,12345, table), t);

            TransactionContext t2 = txnManager.Begin();
            var v3 = table.Read(new KeyAttr(1,56789, table), t2);
            table.Update(new KeyAttr(1,12345, table), BitConverter.GetBytes(5).AsSpan(), t2);

            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);
            txnManager.Terminate();

            Assert.IsTrue(v3.IsEmpty, "New context should not read uncommitted value");
            Assert.IsTrue(success, "Transaction was unable to commit");
            Assert.IsFalse(success2, "Transaction 2 should abort");
            CollectionAssert.AreEqual(v2.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect (W(Tj) U R(Tj)), Tj overlaps with Ti validation or write phase
        /// </summary>
        public void TestWRUnionWIntersect(){
            // Dictionary<long,(bool,int)> schema = new Dictionary<long, (bool,int)>();
            // schema.Add(12345, (false, 4));
            // Table table = new Table(schema);
            // TransactionManager txnManager = new TransactionManager();
            // txnManager.Run();

            // TransactionContext t = txnManager.Begin();
            // table.Upsert(new KeyAttr(1,12345, table), BitConverter.GetBytes(21).AsSpan(), t);
            // // Thread thread = new Thread(() => {
            // //     var success = Commit(txnManager, t);
            // //     Assert.IsTrue(success, "Transaction was unable to commit");
            // // }); 

            // TransactionContext t2 = txnManager.Begin();
            // table.Upsert(new KeyAttr(1,12345, table), BitConverter.GetBytes(5).AsSpan(), t2);
            // // thread.Start();
            // // while (t.status == TransactionStatus.Idle){} // make sure Ti completed read phase
            // var success = txnManager.Commit(t);
            // var success2 = txnManager.Commit(t2);

            // Assert.IsFalse(success2, "Transaction 2 should abort");
            // // bool success = false;

            // // Assert.IsFalse(success);
        }

    }
}