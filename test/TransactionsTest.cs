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
    public unsafe class TransactionTests
    {
        public static int nCommitterThreads = 5;
        // TODO: test diff attributes

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestOversizeInsert(){
            (long,int)[] schema = {(12345,10)};
            Table test = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 10, 0)};
            byte[] name = Encoding.ASCII.GetBytes("Jonathan Doever");
            test.Insert(td, name, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestUndersizeInsert(){
            (long,int)[] schema = {(12345,10)};
            Table test = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 1, 0)};
            byte[] name = Encoding.ASCII.GetBytes("a");
            test.Insert(td, name, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestEmptyInsert(){
            (long,int)[] schema = {(12345,10)};
            Table test = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 10, 0)};
            byte[] name = Encoding.ASCII.GetBytes("");
            test.Insert(td, name, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInsertExistingKey(){
            (long,int)[] schema = {(12345,10)};
            Table test = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 10, 0)};
            byte[] name = Encoding.ASCII.GetBytes("");
            TupleId id = test.Insert(td, name, t);
            test.Insert(id, td, name, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();
        }

        [TestMethod]
        public void TestSingleInsertReadTransaction(){
            (long,int)[] schema = {(12345,4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};
            byte[] value = BitConverter.GetBytes(21);
            TupleId id = table.Insert(td, value, t);
            var v1 = table.Read(id, td, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(value, v1.ToArray());
        }

        [TestMethod]
        public void TestSingleInsertUpdateTransaction(){
            (long,int)[] schema = {(12345,4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};
            byte[] value = BitConverter.GetBytes(21);
            TupleId id = table.Insert(td, value, t);
            value = BitConverter.GetBytes(40);
            table.Update(id, td, value, t);
            var v1 = table.Read(id, td, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(value, v1.ToArray());
        }

        [TestMethod]
        public void TestSerialInsertUpdate(){
            (long,int)[] schema = {(12345,4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};
            TupleId tid = table.Insert(td, BitConverter.GetBytes(21).AsSpan(), t);
            var v1 = table.Read(tid, td, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(v1.ToArray(), BitConverter.GetBytes(21));
        }

        [TestMethod]
        public void TestInsertAllAttributes(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0), new TupleDesc(56789, 4, 4)};
            byte[] value = BitConverter.GetBytes(21);
            byte[] value2 = BitConverter.GetBytes(95);
            
            TupleId id = table.Insert(td, value.Concat(value2).ToArray(), t);
            TupleDesc[] td1 = {new TupleDesc(12345, 4, 0)};
            var v1 = table.Read(id, td1, t);
            TupleDesc[] td2 = {new TupleDesc(56789, 4, 0)};
            var v2 = table.Read(id, td2, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(value, v1.ToArray());
            CollectionAssert.AreEqual(value2, v2.ToArray());
        }

        [TestMethod]
        public void TestInsertSomeAttributes(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td1 = {new TupleDesc(12345, 4, 0)};
            TupleDesc[] td2 = {new TupleDesc(56789, 4, 0)};
            byte[] value = BitConverter.GetBytes(21);
            
            TupleId id = table.Insert(new TupleDesc[]{new TupleDesc(56789, 4, 0)}, value, t);
            var v1 = table.Read(id, td1, t);
            var v2 = table.Read(id, td2, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            Assert.IsTrue(Util.IsEmpty(v1));
            CollectionAssert.AreEqual(value, v2.ToArray());
        }

        [TestMethod]
        public void TestReadAllAttributes(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();

            TransactionContext t = txnManager.Begin();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0), new TupleDesc(56789, 4, 4)};
            byte[] value1 = BitConverter.GetBytes(21);
            byte[] value2 = BitConverter.GetBytes(95);
            byte[] value = value1.Concat(value2).ToArray();
            
            TupleId id = table.Insert(td, value, t);
            var v1 = table.Read(id, td, t);
            var success = txnManager.Commit(t);
            txnManager.Terminate();

            Assert.IsTrue(success, "Transaction was unable to commit");
            CollectionAssert.AreEqual(value, v1.ToArray());
        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), W(Ti) intersects W(Tj) (Key 1), R(Ti) intersects W(Tj) (Key 1)
        /// </summary>
        public void TestWRNoIntersectWWIntersectWRIntersect(){
            (long,int)[] schema = {(12345,4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};

            TransactionContext t = txnManager.Begin();
            byte[] val1 = BitConverter.GetBytes(21);
            TupleId id1 = table.Insert(td, val1, t);
            var res1 = table.Read(id1, td, t);

            TransactionContext t2 = txnManager.Begin();
            var res2 = table.Read(new TupleId(2, table), td, t2);

            byte[] val2 = BitConverter.GetBytes(5);
            table.Update(id1, td, val2, t2);
            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);


            TransactionContext t3 = txnManager.Begin();
            var res3 = table.Read(id1, td, t3);
            var success3 = txnManager.Commit(t3);
            txnManager.Terminate();

            Assert.IsTrue(success2, "Transaction 2 was unable to commit");
            Assert.IsTrue(success3, "Transaction 3 was unable to commit");
            CollectionAssert.AreEqual(res1.ToArray(), val1);
            Assert.IsTrue(Util.IsEmpty(res2));
            CollectionAssert.AreEqual(res3.ToArray(), val2);
        }

        [TestMethod]
        /// <summary>
        /// Should succeed
        /// W(Ti) does not intersect R(Tj), R(Ti) intersects W(Tj), W(Ti) does not intersects W(Tj)
        /// </summary>
        public void TestWRNoIntersectRWIntersectWWNoIntersect(){
            (long,int)[] schema = {(12345,4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};

            TransactionContext t = txnManager.Begin();
            byte[] val1 = BitConverter.GetBytes(21);
            TupleId id1 = table.Insert(td, val1, t);
            TupleId id2 = new TupleId(2, table);
            var res1 = table.Read(id2, td, t);
            // Thread thread = new Thread(() => Commit(txnManager, t)); 

            TransactionContext t2 = txnManager.Begin();
            var res2 = table.Read(id2, td, t2);
            byte[] val2 = BitConverter.GetBytes(5);
            table.Update(id2, td, val2, t2);

            // thread.Start();
            // while (t.status == TransactionStatus.Idle){} // make sure Ti completed read phase
            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);

            TransactionContext t3 = txnManager.Begin();
            var res3 = table.Read(id1, td, t3);
            var res4 = table.Read(id2, td, t3);
            var success3 = txnManager.Commit(t3);
            txnManager.Terminate();

            Assert.IsTrue(success2, "Transaction 2 was unable to commit");
            Assert.IsTrue(success3, "Transaction 3 was unable to commit");
            Assert.IsTrue(Util.IsEmpty(res1));
            Assert.IsTrue(Util.IsEmpty(res2));
            CollectionAssert.AreEqual(res3.ToArray(), BitConverter.GetBytes(21));
            CollectionAssert.AreEqual(res4.ToArray(), BitConverter.GetBytes(5));
        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect R(Tj), Ti doesnt finish writing before Tj starts
        /// </summary>
        public void TestWRIntersectRWIntersectWWNoIntersect(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};
            TupleDesc[] td2 = {new TupleDesc(56789, 4, 4)};

            TransactionContext t = txnManager.Begin();
            byte[] val1 = BitConverter.GetBytes(21);
            TupleId id1 = table.Insert(td, val1, t);
            var res1 = table.Read(id1, td, t);

            TransactionContext t2 = txnManager.Begin();
            byte[] val2 = BitConverter.GetBytes(5);
            var res2 = table.Read(id1, td, t2);
            table.Update(id1, td, val2, t2);

            var success = txnManager.Commit(t);
            var success2 = txnManager.Commit(t2);
            txnManager.Terminate();

            Assert.IsTrue(Util.IsEmpty(res2), "New context should not read uncommitted value");
            Assert.IsTrue(success, "Transaction was unable to commit");
            Assert.IsFalse(success2, "Transaction 2 should abort");
            CollectionAssert.AreEqual(res1.ToArray(), val1);
        }

        [TestMethod]
        /// <summary>
        /// Should abort. This test cases only has W(Ti) insersect W(Tj)
        /// W(Ti) intersect (W(Tj) U R(Tj)), Tj overlaps with Ti validation or write phase
        /// </summary>
        public void TestWRUnionWIntersect(){
            (long,int)[] schema = {(12345,4), (56789, 4)};
            Table table = new Table(schema);
            TransactionManager txnManager = new TransactionManager(nCommitterThreads);
            txnManager.Run();
            TupleDesc[] td = {new TupleDesc(12345, 4, 0)};

            TransactionContext t = txnManager.Begin();
            byte[] val1 = BitConverter.GetBytes(21);
            TupleId id1 = table.Insert(td, val1, t);
            var res1 = table.Read(id1, td, t);

            TransactionContext t2 = txnManager.Begin();
            byte[] val2 = BitConverter.GetBytes(5);
            table.Insert(id1, td, val2, t2);

            txnManager.active.Add(t); // manually "commit" t and ensure it is still ongoing
            var success2 = txnManager.Commit(t2);

            txnManager.Terminate();
            CollectionAssert.AreEqual(res1.ToArray(), val1);
            Assert.IsFalse(success2, "Transaction 2 should abort");
        }

    }
}