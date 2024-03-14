using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using FASTER.common;
using FASTER.darq;
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
    private IWriteAheadLog? wal;
    protected ILogger logger;

    public TransactionManager(int numThreads, IWriteAheadLog? wal = null, ILogger logger = null){
        this.wal = wal;
        this.logger = logger;
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
    public bool Commit(TransactionContext ctx){
        PrintDebug($"adding ctx to queue for commit", ctx);
        ctx.status = TransactionStatus.Pending;
        txnQueue.Add(ctx);        
        while (!Util.IsTerminalStatus(ctx.status)){
            Thread.Yield();
        }
        PrintDebug("TERMINAL STATUS !!", ctx);
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

    public void Reset() {
        txnQueue.CompleteAdding();
        txnQueue = new BlockingCollection<TransactionContext>();
        active = new List<TransactionContext>();
        sl = new SpinLock();
        txnc = 0;
        tid = 0;
        tnumToCtx = new TransactionContext[pastTnumCircularBufferSize];
        ctxPool = new SimpleObjectPool<TransactionContext>(() => new TransactionContext());
    }

    public void Terminate(){
        for (int i = 0; i < committer.Length; i++) {
            committer[i]?.Interrupt();
        }
        if (wal != null) wal.Terminate();
    }

    /// <summary>
    /// Mutates "active", sets fields to mark active transaction
    /// </summary>
    /// <param name="ctx"></param>
    /// <returns></returns>
    public bool Validate(TransactionContext ctx){
        PrintDebug($"Validating own keys", ctx);
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
        // PrintDebug($"Committing {ctx.startTxn} to {finishTxn}", ctx);

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

    public void Write(TransactionContext ctx, Action<long, LogType> commit){
        PrintDebug("Write phase", ctx);
        bool lockTaken = false; // signals if this thread was able to acquire lock
        if (wal != null) {
            wal.Begin(ctx.tid);            
        }
        foreach (var item in ctx.GetWriteset()){
            TupleId tupleId = item.Item1;
            int start = 0;
            foreach (TupleDesc td in item.Item2){
                // TODO: should not throw exception here, but if it does, abort. 
                // failure here means crashed before commit. would need to rollback
                if (this.wal != null) {
                    wal.Write(ctx.tid, new KeyAttr(tupleId.Key, td.Attr, tupleId.Table.GetId()), item.Item3.AsSpan(start, td.Size).ToArray());
                }
                tupleId.Table.Write(new KeyAttr(tupleId.Key, td.Attr, tupleId.Table.GetId()), item.Item3.AsSpan(start, td.Size));
                start += td.Size;
            }
        }
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            commit(ctx.tid, LogType.Commit);
            // wal.Finish(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
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
        PrintDebug("Write phase done", ctx);
    }

    public void Abort(TransactionContext ctx){
        PrintDebug($"Aborting tid {ctx.tid}");
        bool lockTaken = false; // signals if this thread was able to acquire lock
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            wal.Finish(ctx.tid, LogType.Abort);
        }
        try {
            sl.Enter(ref lockTaken);
            active.Remove(ctx);
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
    }

    virtual protected void ValidateAndWrite(TransactionContext ctx) {
        bool valid = Validate(ctx);

        if (valid) {
            ctx.status = TransactionStatus.Validated;
            Write(ctx, (tid, type) => wal.Finish(tid, type));
            ctx.status = TransactionStatus.Committed;
        } else {
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
        }
    }

    virtual public void PrintDebug(string msg, TransactionContext ctx = null){
        logger.LogInformation($"[TM TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }

    private long NewTransactionId(){
        return Interlocked.Increment(ref tid);
    }
}

public class ShardedTransactionManager : TransactionManager {
    private RpcClient rpcClient;
    private IWriteAheadLog wal;
    private ConcurrentDictionary<long, List<(long, long)>> txnIdToOKDarqLsns = new ConcurrentDictionary<long, List<(long, long)>>(); // tid to num shards waiting on
    public ShardedTransactionManager(int numThreads, IWriteAheadLog wal, RpcClient rpcClient, ILogger logger = null) : base(numThreads, wal, logger){
        this.rpcClient = rpcClient;
        this.wal = wal;
    }

    public void MarkAcked(long tid, TransactionStatus status, long darqLsn, long shard){
        TransactionContext? ctx = active.Find(ctx => ctx.tid == tid);
        if (ctx == null) return; // already aborted, ignore
        if (status == TransactionStatus.Aborted){
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
            // TODO: should consume all existing OKs 
            return;
        } else if (status != TransactionStatus.Validated){
            throw new Exception($"Invalid status {status} for tid {tid}");
        }

        txnIdToOKDarqLsns[tid].Add((darqLsn, shard));        
        wal.RecordOk(tid, shard);

        PrintDebug($"Marked acked", ctx);

        if (txnIdToOKDarqLsns[tid].Count == rpcClient.GetNumServers() - 1){
            PrintDebug($"done w validation", ctx);
            ctx.status = TransactionStatus.Validated;
            Write(ctx, (tid, type) => wal.Finish2pc(tid, type, txnIdToOKDarqLsns[tid]));
            ctx.status = TransactionStatus.Committed;
        }
    }

    override protected void ValidateAndWrite(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        // validate own, need a map to track which has responded 
        bool valid = Validate(ctx);
        PrintDebug($"Validate own ctx: {valid}");

        if (valid) {
            // split writeset into shards
            Dictionary<long, List<(KeyAttr, byte[])>> shardToWriteset = new Dictionary<long, List<(KeyAttr, byte[])>>();
            for (int i = 0; i < ctx.GetWriteset().Count; i++){
                var item = ctx.GetWriteset()[i];
                TupleId tupleId = item.Item1;
                TupleDesc[] tds = item.Item2;
                long shardDest = rpcClient.HashKeyToDarqId(tupleId.Key);
                if (rpcClient.GetId() != shardDest){
                    for (int j = 0; j < tds.Length; j++){
                        KeyAttr keyAttr = new KeyAttr(tupleId.Key, tds[j].Attr, tupleId.Table.GetId());
                        if (!shardToWriteset.ContainsKey(shardDest)){
                            shardToWriteset[shardDest] = new List<(KeyAttr, byte[])>();
                        }
                        shardToWriteset[shardDest].Add((keyAttr, item.Item3));
                    }
                }
            }
            if (shardToWriteset.Count > 0) {

                if (txnIdToOKDarqLsns.ContainsKey(ctx.tid)) throw new Exception($"Ctx TID {ctx.tid} already started validating?");
                PrintDebug($"Created waiting for OK list", ctx);
                txnIdToOKDarqLsns[ctx.tid] = new List<(long, long)>();
                for (int shard = 0; shard < rpcClient.GetNumServers(); shard++){
                    if (shard == rpcClient.GetId() || shardToWriteset.ContainsKey(shard)) continue;
                    txnIdToOKDarqLsns[ctx.tid].Add((-1, shard)); // hacky way to indicate that we don't need to wait for this shard
                }
                // send out prepare messages and wait; the commit is finished by calls to MarkAcked
                wal.Prepare(shardToWriteset, ctx.tid);
            } else {
                PrintDebug($"Commit on local, no waiting needed", ctx);
                Write(ctx, (tid, type) => wal.Finish(tid, type));
                ctx.status = TransactionStatus.Committed;
            }
        } else {
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
        }

    }

    public RpcClient GetRpcClient(){
        return rpcClient;
    }

    void PrintDebug(string msg, TransactionContext ctx = null){
        logger.LogInformation($"[STM {rpcClient.GetId()} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
    
}
}