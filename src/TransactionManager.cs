using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using FASTER.common;
using FASTER.libdpr;

namespace DB {
public class TransactionManager {
    internal BlockingCollection<TransactionContext> txnQueue = new BlockingCollection<TransactionContext>();
    internal static int pastTnumCircularBufferSize = 1 << 14;
    internal TransactionContext[] tnumToCtx = new TransactionContext[pastTnumCircularBufferSize]; // write protected by spinlock, atomic with txnc increment
    internal int txnc = 0;
    internal int tid = 0;
    internal Thread[] committer;
    internal SimpleObjectPool<TransactionContext> ctxPool;
    internal List<TransactionContext> active = new List<TransactionContext>(); // list of active transaction contexts, protected by spinlock
    internal SpinLock sl = new SpinLock();
    internal IWriteAheadLog? wal;
    internal ConcurrentDictionary<long, long> txnTbl = new ConcurrentDictionary<long, long>(); // ongoing transactions mapped to most recent lsn

    public TransactionManager(int numThreads, IWriteAheadLog? wal = null){
        this.wal = (DARQWal)wal;
        ctxPool = new SimpleObjectPool<TransactionContext>(() => new TransactionContext());
        committer = new Thread[numThreads];
        for (int i = 0; i < committer.Length; i++) {
            committer[i] = new Thread(() => {
                try {
                    while (true) {
                        TransactionContext ctx = txnQueue.Take();
                        ValidateAndWrite(ctx);
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
    virtual public bool Commit(TransactionContext ctx){
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
        if (wal != null) wal.Terminate();
    }

    public bool Validate(TransactionContext ctx){
        bool lockTaken = false; // signals if this thread was able to acquire lock
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

        // validate
        for (int i = ctx.startTxnNum + 1; i <= finishTxn; i++){
            // Console.WriteLine((i & (pastTnumCircularBufferSize - 1)) + " readset: " + ctx.GetReadset().Count + "; writeset:" + ctx.GetWriteset().Count);
            // foreach (var x in tnumToCtx[i % pastTnumCircularBufferSize].GetWriteset()){
            //     Console.Write($"{x}, ");
            // }
            foreach (var item in ctx.GetReadset()){
                TupleId tupleId = item.Item1;
                // Console.WriteLine($"scanning for {keyAttr}");
                // TODO: rename keyattr since tupleid is redundant
                if (tnumToCtx[i & (pastTnumCircularBufferSize - 1)].InWriteSet(tupleId)){
                    return false;
                }
            }
        }
        
        foreach (TransactionContext pastTxn in finish_active){
            foreach (var item in pastTxn.GetWriteset()){
                TupleId tupleId = item.Item1;
                if (ctx.InReadSet(tupleId) || ctx.InWriteSet(tupleId)){
                    // Console.WriteLine($"ABORT because conflict: {keyAttr}");
                    return false;
                }
            }
        }
        return true;
    }

    private void ValidateAndWrite(TransactionContext ctx) {
        bool valid = Validate(ctx);
        bool lockTaken = false; // signals if this thread was able to acquire lock

        ctx.status = TransactionStatus.Validated;
        StepRequestBuilder requestBuilder;
        if (wal != null) {
            var res = wal.Begin(ctx.tid);
            long writtenLsn = res.Item1;
            requestBuilder = res.Item2;
            txnTbl[ctx.tid] = writtenLsn;
        }
        if (valid) {
            // write phase
            foreach (var item in ctx.GetWriteset()){
                TupleId tupleId = item.Item1;
                int start = 0;
                foreach (TupleDesc td in item.Item2){
                    // TODO: should not throw exception here, but if it does, abort. 
                    // failure here means crashed before commit. would need to rollback
                    if (this.wal != null) {
                        wal.Write(new LogEntry(txnTbl[ctx.tid], ctx.tid, new KeyAttr(tupleId.Key, td.Attr, tupleId.Table), item.Item3.AsSpan(start, td.Size).ToArray()), requestBuilder);
                        // wal.Log(new LogEntry(txnTbl[ctx.tid], ctx.tid, new KeyAttr(tupleId.Key, td.Attr, tupleId.Table), val));
                    }
                    tupleId.Table.Write(new KeyAttr(tupleId.Key, td.Attr, tupleId.Table), item.Item3.AsSpan(start, td.Size));
                    start += td.Size;
                }
            }
            // TODO: verify that should be logged before removing from active
            if (wal != null){
                long prevLsn = txnTbl[ctx.tid];
                
                txnTbl.TryRemove(ctx.tid, out _);
                wal.Commit(new LogEntry(prevLsn, ctx.tid, LogType.Commit), requestBuilder);
            }
            // assign num 
            int finalTxnNum;
            try {
                sl.Enter(ref lockTaken);
                txnc += 1; // TODO: deal with int overflow
                finalTxnNum = txnc;
                active.Remove(ctx);
                if (tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] != null){ 
                    ctxPool.Return(tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)]);
                }
                tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] = ctx;
            } finally {
                if (lockTaken) sl.Exit();
                lockTaken = false;
            }

            ctx.status = TransactionStatus.Committed;
        } else {
            // TODO: verify that should be logged before removing from active
            if (wal != null){
                long prevLsn = txnTbl[ctx.tid];
                txnTbl.TryRemove(ctx.tid, out _);
                wal.Commit(new LogEntry(prevLsn, ctx.tid, LogType.Abort), requestBuilder);
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

public class ShardedTransactionManager : TransactionManager {
    public ShardedTransactionManager(int numThreads, IWriteAheadLog? wal) : base(numThreads, wal){
    }

    public override bool Commit(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        // TODO: add prepare message to WAL 
        
        return false;
    }
    
}
}