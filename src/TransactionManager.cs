using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using FASTER.common;

namespace DB {
public class TransactionManager {
    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal static int pastTnumCircularBufferSize = 1 << 14;
    internal TransactionContext[] tnumToCtx = new TransactionContext[pastTnumCircularBufferSize]; // not protected because writes are only done by one thread
    internal int txnc = 0;
    internal int tid = 0;
    internal Thread[] committer;
    internal SimpleObjectPool<TransactionContext> ctxPool;
    internal List<TransactionContext> active = new List<TransactionContext>(); // list of active transaction contexts, protected by spinlock
    internal SpinLock sl = new SpinLock();
    internal IWriteAheadLog? wal;
    internal ConcurrentDictionary<long, long> txnTbl = new ConcurrentDictionary<long, long>(); // ongoing transactions mapped to most recent lsn

    public TransactionManager(int numThreads, IWriteAheadLog? wal = null){
        this.wal = wal;
        ctxPool = new SimpleObjectPool<TransactionContext>(() => new TransactionContext());
        committer = new Thread[numThreads];
        for (int i = 0; i < committer.Length; i++) {
            committer[i] = new Thread(() => {
                try {
                    while (true) {
                        TransactionContext ctx = txnQueue.Take();
                        validateAndWrite(ctx);
                    }
                } catch (ThreadInterruptedException){
                    System.Console.WriteLine("Terminated");
                }
            });
        }
    }

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <returns>Newly created transaction context</returns>
    public TransactionContext Begin(){
        var ctx = ctxPool.Checkout();
        ctx.Init(startTxn: txnc, NewTransactionId());

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
        if (wal != null) {
            long writtenLsn = wal.Log(new LogEntry(-1, ctx.tid, LogType.Begin));
            txnTbl[ctx.tid] = writtenLsn;
        }
        if (valid) {
            // write phase
            foreach (var item in ctx.GetWriteset()){
                byte[] val = item.Item2;
                KeyAttr keyAttr = item.Item1;
                // TODO: should not throw exception here, but if it does, abort. 
                // failure here means crashed before commit. would need to rollback
                if (this.wal != null) {
                    wal.Log(new LogEntry(txnTbl[ctx.tid], ctx.tid, new Operation(OperationType.Update, new TupleId(keyAttr.Key, this.GetHashCode()), new TupleDesc[]{new TupleDesc(keyAttr.Attr, val.Length)}, val)));
                }
                keyAttr.Table.Write(keyAttr, val);
            }
            // TODO: verify that should be logged before removing from active
            if (wal != null){
                long prevLsn = txnTbl[ctx.tid];
                
                txnTbl.TryRemove(ctx.tid, out _);
                wal.Log(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
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
            if (wal != null){
                long prevLsn = txnTbl[ctx.tid];
                txnTbl.TryRemove(ctx.tid, out _);
                wal.Log(new LogEntry(prevLsn, ctx.tid, LogType.Abort));
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
    }

    private long NewTransactionId(){
        return Interlocked.Increment(ref tid);
    }
}

}