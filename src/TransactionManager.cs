using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace DB {
public class TransactionManager {
    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal static int pastTnumCircularBufferSize = 1 << 14;
    internal TransactionContext[] tnumToCtx = new TransactionContext[pastTnumCircularBufferSize];
    internal int txnc = 0;
    internal int tid = 0;
    internal Thread[] committer;
    internal ObjectPool<TransactionContext> ctxPool = ObjectPool.Create<TransactionContext>();
    internal List<TransactionContext> active = new List<TransactionContext>(); // list of active transaction contexts
    internal SpinLock sl = new SpinLock();
    internal LogWAL logWAL;
    internal Dictionary<long, long> txnTbl = new Dictionary<long, long>(); // ongoing transactions mapped to most recent lsn

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

    public TransactionManager(int numThreads, LogWAL logWAL) : this(numThreads){
        this.logWAL = logWAL;
    }

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <returns>Newly created transaction context</returns>
    public TransactionContext Begin(){
        var ctx = ctxPool.Get();
        ctx.Init(startTxn: txnc, NewTransactionId());
        if (logWAL != null) {
            long writtenLsn = logWAL.Invoke(new LogEntry(-1, ctx.tid, LogType.Begin));
            txnTbl[ctx.tid] = writtenLsn;
        }

        return ctx;
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
        // Console.WriteLine($"curr tnums: {{{string.Join(Environment.NewLine, tnumToCtx)}}}");
        for (int i = ctx.startTxnNum + 1; i <= finishTxn; i++){
            // Console.WriteLine(i + " readset: " + ctx.GetReadset().Count + "; writeset:" + ctx.GetWriteset().Count);
            // foreach (var x in tnumToCtx[i % pastTnumCircularBufferSize].GetWriteset()){
            //     Console.Write($"{x.Key}, ");
            // }
            foreach (var item in ctx.GetReadset()){
                KeyAttr keyAttr = item.Item1;
                // Console.WriteLine($"scanning for {keyAttr}");
                if (tnumToCtx[i & (pastTnumCircularBufferSize - 1)].GetWriteSetKeyIndex(keyAttr) != -1){
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
                // TODO: should not throw exception here, but if it does, abort. 
                // failure here means crashed before commit. would need to rollback
                if (this.logWAL != null) {
                    logWAL.Invoke(new LogEntry(txnTbl[ctx.tid], ctx.tid, new Operation(OperationType.Update, new TupleId(keyAttr.Key, this.GetHashCode()), new TupleDesc[]{new TupleDesc(keyAttr.Attr, val.Length)}, val)));
                }
                keyAttr.Table.Write(keyAttr, val);
            }
            // TODO: verify that should be logged before removing from active
            if (logWAL != null){
                long prevLsn = txnTbl[ctx.tid];
                txnTbl.Remove(ctx.tid);
                logWAL.Invoke(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
            }
            // assign num 
            int finalTxnNum;
            try {
                sl.Enter(ref lockTaken);
                txnc += 1; // TODO: deal with int overflow
                finalTxnNum = txnc;
                active.Remove(ctx);
            } finally {
                if (lockTaken) sl.Exit();
                lockTaken = false;
            }

            if (tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] != null){ 
                ctxPool.Return(tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)]);
            }
            tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] = ctx;
            ctx.status = TransactionStatus.Committed;
        } else {
            // TODO: verify that should be logged before removing from active
            if (logWAL != null){
                long prevLsn = txnTbl[ctx.tid];
                txnTbl.Remove(ctx.tid);
                logWAL.Invoke(new LogEntry(prevLsn, ctx.tid, LogType.Abort));
            }
            try {
                sl.Enter(ref lockTaken);
                active.Remove(ctx);
            } finally {
                if (lockTaken) sl.Exit();
                lockTaken = false;
            }

            ctx.status = TransactionStatus.Aborted;
        }
        ctx.mre.Set();
    }

    private long NewTransactionId(){
        return Interlocked.Increment(ref tid);
    }
}

}