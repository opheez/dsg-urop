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
            schema.Add(67890, (false, 32));

            Table table = new Table(schema);
            TransactionManager.Run();
            TransactionContext t = TransactionManager.Begin(table);
            var v3 = table.Upsert(new KeyAttr(1,12345), BitConverter.GetBytes(21).AsSpan(), t);
            var v1 = table.Read(new KeyAttr(1,12345), t);
            // var v2 = table.Read(new KeyAttr(2,12345), t);
            var success = TransactionManager.Commit(t);
            Assert.IsTrue(success);
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
            bool success = false;
            Assert.IsFalse(success);
        }

        [TestMethod]
        /// <summary>
        /// Should abort
        /// W(Ti) intersect (W(Tj) U R(Tj)), Tj overlaps with Ti validation or write phase
        /// </summary>
        public void TestWRUnionWIntersect(){
            bool success = false;

            Assert.IsFalse(success);
        }

    }
}