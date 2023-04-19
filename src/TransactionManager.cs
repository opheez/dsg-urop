using System.Collections.Concurrent;
using System.Threading;

namespace DB {
public class TransactionManager {

    internal static ConcurrentQueue<TransactionContext> txnToValidate;
    internal static ConcurrentQueue<TransactionContext> txnToCommit;
    public static TransactionContext Begin(){
        return new TransactionContext();
    }

    // blocks until commit is completed
    public static bool Commit(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        
        return false;
    }

    public static void Run(){
        // spawns a thread that continuously pulls of the queue
        Thread t = new Thread(() => {
            // for each TransactionContext object, 
            //  check all other TransactionContexts if 
            //  validate read set by checking if value changed TODO: should there be some version #? or just based on value?
            //  write phase: 

        });
        t.Start();
    }
}

}