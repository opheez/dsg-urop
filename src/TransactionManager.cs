using System.Collections;
using System.Collections.Concurrent;
using System.Threading;

namespace DB {
public class TransactionManager {

    internal static ConcurrentQueue<TransactionContext> txnQueue = new ConcurrentQueue<TransactionContext>();
    internal static Dictionary<uint, TransactionContext> tidToCtx = new Dictionary<uint, TransactionContext>();
    internal static uint txnc = 0;

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <param name="tbl">Table that the transaction context belongs to</param>
    /// <returns>Newly created transaction context</returns>
    public static TransactionContext Begin(){
        return new TransactionContext(txnc);
    }

    /// <summary>
    /// Submit a transaction context to be committed. Blocks until commit is completed
    /// </summary>
    /// <param name="ctx">Context to commit</param>
    /// <returns>True if the transaction committed, false otherwise</returns>
    public static bool Commit(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        txnQueue.Enqueue(ctx);
        while (ctx.status == TransactionStatus.Pending){
            Thread.Sleep(100);
        }
        if (ctx.status == TransactionStatus.Aborted){
            return false;
        } else if (ctx.status == TransactionStatus.Committed) {
            return true;
        }
        return false;
    }

    /// <summary>
    /// Spawns a thread that continuously polls the queue to 
    /// validate and commit a transaction context
    /// </summary>
    public static void Run(){
        Thread committer = new Thread(() => {
            while (true) {
                if (txnQueue.TryDequeue(out TransactionContext ctx)){
                    uint finishTxn = txnc;
                    bool valid = true;
                    // validate
                    for (uint i = ctx.startTxn + 1; i <= finishTxn; i++){
                        if (tidToCtx[i].GetWriteset().Keys.Intersect(ctx.GetReadset().Keys).Count() != 0) {
                            valid = false;
                            break;
                        }
                    }
                    if (valid) {
                        // write phase
                        foreach (var item in ctx.GetWriteset()){
                            KeyAttr keyAttr = item.Key;
                            byte[] val = item.Value;
                            keyAttr.Table.Upsert(keyAttr.Key, keyAttr.Attr, val.AsSpan());
                        }
                        Interlocked.Increment(ref txnc);
                        tidToCtx[txnc] = ctx;
                        // assign num 
                        ctx.status = TransactionStatus.Committed;
                    } else {
                        ctx.status = TransactionStatus.Aborted;
                    }
                } else {
                    Thread.Sleep(50);
                }
            }
        });

        committer.Start();

    }
}

}