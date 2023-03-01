// using Microsoft.VisualStudio.TestTools.UnitTesting;

// using TableNS;

// namespace TableTests
// {
//     [TestClass]
//     public class TableTests
//     {
//         [TestMethod]
//         public void TestMethod1()
//         {
//             Dictionary<string,int> schema = new Dictionary<string, int>();
//             schema.Add("name", 100);
//             schema.Add("age", 32);

//             Table test = new Table(schema);
//             foreach (var x in test.catalog){
//                 Console.WriteLine(x);
//             }
//             byte[] name = new byte[] {(byte)'o'};

//             test.Set("a", "name", name);
//             var y = test.Get("a", "name");
//             Console.WriteLine(test.Get("a", "name").ToString());
//         }
//     }
// }