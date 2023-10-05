using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace DB {
public class TransactionManager {

    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal static int pastTidCircularBufferSize = 10000;
    internal TransactionContext[] tidToCtx = new TransactionContext[pastTidCircularBufferSize];
    internal int txnc = 0;
    internal Thread committer;
    internal ObjectPool<TransactionContext> ctxPool = ObjectPool.Create<TransactionContext>();

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <param name="tbl">Table that the transaction context belongs to</param>
    /// <returns>Newly created transaction context</returns>
    public TransactionContext Begin(){
        var ctx = ctxPool.Get();
        ctx.Init(txnc);
        return ctx;
        // return new TransactionContext(txnc);
    }

    /// <summary>
    /// Submit a transaction context to be committed. Blocks until commit is completed
    /// </summary>
    /// <param name="ctx">Context to commit</param>
    /// <returns>True if the transaction committed, false otherwise</returns>
    public bool Commit(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        txnQueue.Add(ctx);        
        ctx.mre.WaitOne();
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
    public void Run(){
        committer = new Thread(() => {
            try {
                while (true) {
                    TransactionContext ctx = txnQueue.Take();
                    int finishTxn = txnc;
                    bool valid = true;
                    // validate
                    // Console.WriteLine($"curr tids: {{{string.Join(Environment.NewLine, tidToCtx)}}}");
                    for (int i = ctx.startTxn + 1; i <= finishTxn; i++){
                        // Console.WriteLine(i + " readset: " + ctx.GetReadset().Count + "; writeset:" + ctx.GetWriteset().Count);
                        // foreach (var x in tidToCtx[i % pastTidCircularBufferSize].GetWriteset()){
                        //     Console.Write($"{x.Key}, ");
                        // }
                        foreach (var item in ctx.GetReadset()){
                            KeyAttr keyAttr = item.Item1;
                            // Console.WriteLine($"scanning for {keyAttr}");
                            if (tidToCtx[i % pastTidCircularBufferSize].GetWriteSetKeyIndex(keyAttr) != -1){
                                valid = false;
                                break;
                            }
                        }
                        if (!valid){
                            break;
                        }
                    }
                    ctx.status = TransactionStatus.Validated;
                    if (valid) {
                        // write phase
                        foreach (var item in ctx.GetWriteset()){
                            byte[] val = item.Item2;
                            KeyAttr keyAttr = item.Item1;
                            // should not throw exception here, but if it does, abort. 
                            // failure here means crashed before commit. would need to rollback
                            keyAttr.Table.Write(keyAttr, val);
                        }
                        // assign num 
                        // wait on 
                        Interlocked.Increment(ref txnc); // TODO: deal with int overflow
                        if (tidToCtx[txnc % pastTidCircularBufferSize] != null){
                            ctxPool.Return(tidToCtx[txnc % pastTidCircularBufferSize]);
                        }
                        tidToCtx[txnc % pastTidCircularBufferSize] = ctx;
                        ctx.status = TransactionStatus.Committed;
                    } else {
                        ctx.status = TransactionStatus.Aborted;
                    }
                    ctx.mre.Set();
                }
            } catch (ThreadInterruptedException e){
                System.Console.WriteLine("Terminated");
            }
        });

        committer.Start();
    }

    public void Terminate(){
        if (committer != null){
            committer.Interrupt();
        }
        
    }
}

}