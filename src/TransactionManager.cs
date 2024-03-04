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
    public bool Commit(TransactionContext ctx){
        Console.WriteLine("adding ctx to queue for commit");
        ctx.status = TransactionStatus.Pending;
        txnQueue.Add(ctx);        
        while (!Util.IsTerminalStatus(ctx.status)){
            Thread.Yield();
        }
        Console.WriteLine("TERMINAL STATUS !!");
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

    public void Write(TransactionContext ctx){
        bool lockTaken = false; // signals if this thread was able to acquire lock
        if (wal != null) {
            var res = wal.Begin(ctx.tid);
            long writtenLsn = res;
            txnTbl[ctx.tid] = writtenLsn;
        }
        foreach (var item in ctx.GetWriteset()){
            TupleId tupleId = item.Item1;
            int start = 0;
            foreach (TupleDesc td in item.Item2){
                // TODO: should not throw exception here, but if it does, abort. 
                // failure here means crashed before commit. would need to rollback
                if (this.wal != null) {
                    wal.Write(new LogEntry(txnTbl[ctx.tid], ctx.tid, new KeyAttr(tupleId.Key, td.Attr, tupleId.Table), item.Item3.AsSpan(start, td.Size).ToArray()));
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
            wal.Commit(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
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
    }

    public void Abort(TransactionContext ctx){
        bool lockTaken = false; // signals if this thread was able to acquire lock
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            long prevLsn = txnTbl[ctx.tid];
            txnTbl.TryRemove(ctx.tid, out _);
            wal.Commit(new LogEntry(prevLsn, ctx.tid, LogType.Abort));
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
            Write(ctx);
            ctx.status = TransactionStatus.Committed;
        } else {
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
        }
    }

    private long NewTransactionId(){
        return Interlocked.Increment(ref tid);
    }
}

public class ShardedTransactionManager : TransactionManager {
    private RpcClient rpcClient;
    private ConcurrentDictionary<long, ConcurrentDictionary<long, TransactionStatus>> acked = new ConcurrentDictionary<long, ConcurrentDictionary<long, TransactionStatus>>();
    public ShardedTransactionManager(int numThreads, IWriteAheadLog wal, RpcClient rpcClient) : base(numThreads, wal){
        this.rpcClient = rpcClient;
    }

    public void MarkAcked(long tid, TransactionStatus status, long shard){
        if (status == TransactionStatus.Aborted){
            active[(int)tid].status = TransactionStatus.Aborted;
            return;
        }

        if (!acked.ContainsKey(tid)){
            acked.TryAdd(tid, new ConcurrentDictionary<long, TransactionStatus>());
        }
        acked[tid][shard] = status;
        Console.WriteLine($"Marked acked {tid} from {shard} with status {status}");

        if (acked[tid].Count == rpcClient.GetNumServers() - 1){
            TransactionContext ctx = active.Find(ctx => ctx.tid == tid);
            if (ctx == null) throw new Exception("ctx not found");
            ctx.status = TransactionStatus.Validated;
        }
    }

    override protected void ValidateAndWrite(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        // validate own, need a map to track which has responded 
        bool valid = Validate(ctx);
        Console.WriteLine($"Validate own ctx: {valid}");

        // send via rpcClient to all shards
        // does same thing except on success, sends out message of ACK success
        Dictionary<long, List<(KeyAttr, byte[])>> shardToWriteset = new Dictionary<long, List<(KeyAttr, byte[])>>();
        for (int i = 0; i < ctx.GetReadset().Count; i++){
            TupleId tupleId = ctx.GetReadset()[i].Item1;

            if (rpcClient.GetId() != rpcClient.HashKeyToDarqId(tupleId.Key)){
                shardToWriteset.TryAdd(rpcClient.HashKeyToDarqId(tupleId.Key), new List<(KeyAttr, byte[])>());
            }
        }
        for (int i = 0; i < ctx.GetWriteset().Count; i++){
            var item = ctx.GetWriteset()[i];
            TupleId tupleId = item.Item1;
            TupleDesc[] tds = item.Item2;
            long shardDest = rpcClient.HashKeyToDarqId(tupleId.Key);

            for (int j = 0; j < tds.Length; j++){
                KeyAttr keyAttr = new KeyAttr(tupleId.Key, tds[j].Attr, tupleId.Table);
                if (rpcClient.GetId() != shardDest){
                    if (!shardToWriteset.ContainsKey(shardDest)){
                        shardToWriteset.Add(rpcClient.HashKeyToDarqId(tupleId.Key), new List<(KeyAttr, byte[])>());
                    }
                    shardToWriteset[shardDest].Add((keyAttr, item.Item3));
                }
            }
        }

        foreach (var shard in shardToWriteset) {
            long darqId = shard.Key;
            List<(KeyAttr, byte[])> writeset = shard.Value;
            LogEntry outEntry = new LogEntry(0, ctx.tid, writeset.Select(x => x.Item1).ToArray(),  writeset.Select(x => x.Item2).ToArray());
            DARQWal wal = (DARQWal)this.wal; // TODO: hacky, fix
            Console.WriteLine($"sending prepare msg to {darqId} with keys {string.Join(", ", writeset.Select(x => x.Item1))}");
            wal.Send(new DarqId(darqId), outEntry);
            

        }

        Console.WriteLine("waiting for validation...");
        while (ctx.status != TransactionStatus.Validated || !Util.IsTerminalStatus(ctx.status)){
            Thread.Yield();
        }
        Console.WriteLine("DONE!");

        if (ctx.status == TransactionStatus.Validated) {
            Write(ctx);
            ctx.status = TransactionStatus.Committed;
        } else {
            Abort(ctx);
        }

    }
    
}
}