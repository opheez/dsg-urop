// using System.Runtime.InteropServices;

// namespace DB {
// /// <summary>
// /// 
// /// </summary>
// public class TransactionContext {

//     internal TransactionStatus status;
//     internal Dictionary<KeyAttr, (bool, byte[])> RWset;

//     public byte[]? Get(KeyAttr keyAttr){
//         if (RWset.ContainsKey(keyAttr)){
//             return RWset[keyAttr];
//         }
//         return null;
//     }

//     public void Set(KeyAttr keyAttr, Span<byte> val){
//         RWset[keyAttr] = (true, val);
//     }

//     public Dictionary<KeyAttr, byte[]> GetReadset(){
//         return RWset.Where(item => !item.Value.Item1).ToDictionary(item => item.Key, item => item.Value.Item2);
//     }
//     public Dictionary<KeyAttr, byte[]> GetWriteset(){
//         return RWset.Where(item => item.Value.Item1).ToDictionary(item => item.Key, item => item.Value.Item2);
//     }
// }

// }