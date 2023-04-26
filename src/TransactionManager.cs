using System.Collections;
using System.Collections.Concurrent;
using System.Threading;

namespace DB {
public class TransactionManager {

    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal Dictionary<uint, TransactionContext> tidToCtx = new Dictionary<uint, TransactionContext>();
    internal uint txnc = 0;

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <param name="tbl">Table that the transaction context belongs to</param>
    /// <returns>Newly created transaction context</returns>
    public TransactionContext Begin(){
        return new TransactionContext(txnc);
    }

    /// <summary>
    /// Submit a transaction context to be committed. Blocks until commit is completed
    /// </summary>
    /// <param name="ctx">Context to commit</param>
    /// <returns>True if the transaction committed, false otherwise</returns>
    public bool Commit(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        txnQueue.Add(ctx);
        Monitor.Enter(ctx);
        Monitor.Wait(ctx);
        if (ctx.status == TransactionStatus.Aborted){
            return false;
        } else if (ctx.status == TransactionStatus.Committed) {
            return true;
        }
        Monitor.Exit(ctx);
        return false;
    }

    /// <summary>
    /// Spawns a thread that continuously polls the queue to 
    /// validate and commit a transaction context
    /// </summary>
    public void Run(){
        Thread committer = new Thread(() => {
            while (true) {
                TransactionContext ctx = txnQueue.Take();
                uint finishTxn = txnc;
                bool valid = true;
                // validate
                // System.Console.WriteLine("My readset: " + ctx.GetReadset().Count + "\nMy writeset:" + ctx.GetWriteset().Count);
                for (uint i = ctx.startTxn + 1; i <= finishTxn; i++){
                    // System.Console.WriteLine(i + " readset: " + ctx.GetReadset().Count + "; writeset:" + ctx.GetWriteset().Count);
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
                    Monitor.Enter(ctx);
                    ctx.status = TransactionStatus.Committed;
                    Monitor.Pulse(ctx);
                    Monitor.Exit(ctx);
                } else {
                    Monitor.Enter(ctx);
                    ctx.status = TransactionStatus.Aborted;
                    Monitor.Pulse(ctx);
                    Monitor.Exit(ctx);
                }
            }
        });

        committer.Start();

    }

    // internal class KeyAttrComparer : IEqualityComparer<KeyAttr> {
    //     public bool Equals(KeyAttr k1, KeyAttr k2)
    //     {
    //         return k1.Equals(k2);
    //     }
        
    //     public int GetHashCode(KeyAttr k)
    //     {
    //         return k.GetHashCode();
    //     }
    // }
}

}