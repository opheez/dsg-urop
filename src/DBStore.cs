//** Tianyu: remove unused files

// using System.Collections.Concurrent;

// public class DBStore<Key> : Table<Key>{
//     internal ConcurrentQueue<TransactionContext> txnToValidate;
//     internal ConcurrentQueue<TransactionContext> txnToCommit;

//     public void Run(){
//         // spawns a thread that continuously pulls of the queue
//         // for each TransactionContext object, 
//         //  check all other TransactionContexts if 
//         //  validate read set by checking if value changed TODO: should there be some version #? or just based on value?
//         //  write phase: 
//     }


// }