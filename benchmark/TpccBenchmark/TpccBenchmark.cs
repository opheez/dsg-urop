// using System.Text;
// using DB;
// using SharpNeat.Utility;

// /// <summary>
// /// Adapted from https://github.com/SQLServerIO/TPCCBench/blob/master/TPCCDatabaseGenerator/TPCCGenData.cs
// /// </summary>
// public class TpccBenchmark {

//     private static readonly FastRandom Frnd = new FastRandom();
//     private static byte[] RandHold = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
//     private static byte[] ZipRandHold = Encoding.ASCII.GetBytes("1234567890");
//     private static byte SpaceAsByte = (byte)' ';
//     public int MaxNumWh;
//     public int NumWh;
//     public static void PopulateWarehouseTable(){
//         var schema = WAREHOUSE_SCHEMA.Select(x => (x.Item1, x.Item2)).ToArray();
//         var table = new Table((int)TableType.Warehouse, schema);
//         int i = NumWh;

//         while (i < MaxNumWh + 1)
//         {
//             byte[] data = new byte[schema.Length];
//             Span<byte> span = new Span<byte>(data);
            
//             int offset = 0;
//             RandomByteString(6, 10).CopyTo(span);
//             offset += 10;
//             RandomByteString(10, 20).CopyTo(span.Slice(offset));
//             offset += 20;
//             RandomByteString(10, 20).CopyTo(span.Slice(offset));
//             offset += 20;
//             RandomByteString(10, 20).CopyTo(span.Slice(offset));
//             offset += 20;
//             RandomByteString(2, 2).CopyTo(span.Slice(offset));
//             offset += 2;
//             RandZip().CopyTo(span.Slice(offset));
//             offset += 9;
//             BitConverter.GetBytes(0.1000).CopyTo(span.Slice(offset));
//             offset += 4;
//             BitConverter.GetBytes(3000000.00).CopyTo(span.Slice(offset));
//             offset += 4;
//             BitConverter.GetBytes(i).CopyTo(span.Slice(offset));
//             table.Insert(data);
            
//             i++;
//         }
//     }



//     public void Run(){
        
//     }
    
//     /// <summary>
//     /// Generates a random byte array with the given length, 
//     /// padded with spaces until it reaches the maximum length 
//     /// </summary>
//     /// <param name="strMin">
//     ///   minimum size of the string
//     /// </param>
//     /// <param name="strMax"></param>
//     /// <returns>Random string</returns>
//     private static byte[] RandomByteString(int strMin, int strMax)
//     {
//         byte[] randomString = new byte[strMax];
//         int stringLen = Frnd.Next(strMin, strMax);
//         for (int x = 0; x < strMax; ++x)
//         {
//             if (x < stringLen)
//                 randomString[x] = RandHold[Frnd.Next(0, 62)];
//             else
//                 randomString[x] = SpaceAsByte;
//         }

//         return randomString;
//     }

//     /// <summary>
//     /// Generates a random zip code byte array with the given length
//     /// </summary>
//     /// <returns>Random string</returns>
//     private static byte[] RandZip()
//     {
//         byte[] holdZip = new byte[5];
//         for (int x = 0; x < 4; ++x)
//         {
//             holdZip[x] = ZipRandHold[Frnd.Next(0, 9)];
//         }
//         for (int x = 4; x < 5; ++x)
//         {
//             holdZip[x] = (byte)'1';
//         }

//         return holdZip;
//     }

// }