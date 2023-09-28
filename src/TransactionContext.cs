using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxn;
    internal Dictionary<TupleId, Operation> Rset;
    internal Dictionary<TupleId, Operation> Wset;
    public ManualResetEvent mre = new ManualResetEvent(false);

    public TransactionContext(){

    }
    public void Init(int startTxn){
        this.startTxn = startTxn;
        mre.Reset();
        status = TransactionStatus.Idle;
        Rset = new Dictionary<TupleId, Operation>(new OCCComparer());
        Wset = new Dictionary<TupleId, Operation>(new OCCComparer());
    }

    public byte[]? GetFromContext(TupleId tupleId){
        Operation? val = null;
        if (Wset.ContainsKey(tupleId)){
            val = Wset[tupleId];
        } else if (Rset.ContainsKey(tupleId)){
            val = Rset[tupleId];
        }
        if (val != null) {
            SetInContext(val.Value.Type, tupleId, val.Value.TupleDescs, val.Value.Value);
        }
        return val.Value.Value;
    }

    public void SetInContext(OperationType op, TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        if (op == OperationType.Read) {
            Rset[tupleId] = new Operation(op, tupleId, tupleDescs, val);
        } else {
            Wset[tupleId] = new Operation(op, tupleId, tupleDescs, val);
        }
    }

    public Dictionary<TupleId, Operation> GetReadset(){
        return Rset;
    }
    public Dictionary<TupleId, Operation> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}