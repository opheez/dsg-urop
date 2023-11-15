using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace DB {
public class TransactionManager {

    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal static int pastTidCircularBufferSize = 1 << 14;
    internal TransactionContext[] tidToCtx = new TransactionContext[pastTidCircularBufferSize];
    internal int txnc = 0;
    internal Thread[] committer;
    internal ObjectPool<TransactionContext> ctxPool = ObjectPool.Create<TransactionContext>();
    internal List<TransactionContext> active = new List<TransactionContext>(); // list of active transaction contexts
    internal SpinLock sl = new SpinLock();

    public TransactionManager(int numThreads){
        committer = new Thread[numThreads];
        for (int i = 0; i < committer.Length; i++) {
            committer[i] = new Thread(() => {
                try {
                    while (true) {
                        TransactionContext ctx = txnQueue.Take();
                        // TODO: assign ctx.id ???? 
                        validateAndWrite(ctx);
                    }
                } catch (ThreadInterruptedException e){
                    System.Console.WriteLine("Terminated");
                }
            });
        }
    }

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
        while (!Util.IsTerminalStatus(ctx.status)){
            Thread.Yield();
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
    public void Run(){
        for (int i = 0; i < committer.Length; i++) {
            committer[i].Start();
        }
    }

    public void Terminate(){
        for (int i = 0; i < committer.Length; i++) {
            committer[i]?.Interrupt();
        }
    }

    private void validateAndWrite(TransactionContext ctx) {
        bool lockTaken = false;
        int finishTxn;
        List<TransactionContext> finish_active;
        try {
            sl.Enter(ref lockTaken);
            finishTxn = txnc;
            finish_active = new List<TransactionContext>(active);
            active.Add(ctx);
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
        // Console.WriteLine($"Committing {ctx.startTxn} to {finishTxn}, ctx: {ctx}");

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
                if (tidToCtx[i & (pastTidCircularBufferSize - 1)].GetWriteSetKeyIndex(keyAttr) != -1){
                    valid = false;
                    break;
                }
            }
            if (!valid){
                break;
            }
        }
        
        foreach (TransactionContext pastTxn in finish_active){
            foreach (var item in pastTxn.GetWriteset()){
                KeyAttr keyAttr = item.Item1;
                if (ctx.GetReadsetKeyIndex(keyAttr) != -1 || ctx.GetWriteSetKeyIndex(keyAttr) != -1){
                    // Console.WriteLine($"ABORT because conflict: {keyAttr}");
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
            try {
                sl.Enter(ref lockTaken);
                txnc += 1; // TODO: deal with int overflow
                ctx.id = txnc;
                active.Remove(ctx);
                if (tidToCtx[ctx.id & (pastTidCircularBufferSize - 1)] != null){ 
                    ctxPool.Return(tidToCtx[ctx.id & (pastTidCircularBufferSize - 1)]);
                }
                tidToCtx[ctx.id & (pastTidCircularBufferSize - 1)] = ctx;
            } finally {
                if (lockTaken) sl.Exit();
                lockTaken = false;
            }

            ctx.status = TransactionStatus.Committed;
        } else {
            try {
                sl.Enter(ref lockTaken);
                active.Remove(ctx);
            } finally {
                if (lockTaken) sl.Exit();
                lockTaken = false;
            }

            ctx.status = TransactionStatus.Aborted;
        }
    }
}

}