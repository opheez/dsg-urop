// using System.Collections.Concurrent;

// namespace DB {
// public class TransactionManager {

//     internal ConcurrentQueue<TransactionContext> txnToValidate;
//     internal ConcurrentQueue<TransactionContext> txnToCommit;
//     public void Begin(){
//         // create a new transaction context object
//     }

//     // blocks until commit is completed
//     public void Commit(){
//         // send to DB Store
//     }

//     public void Run(){
//         // spawns a thread that continuously pulls of the queue
//         // for each TransactionContext object, 
//         //  check all other TransactionContexts if 
//         //  validate read set by checking if value changed TODO: should there be some version #? or just based on value?
//         //  write phase: 
//     }
// }

// }