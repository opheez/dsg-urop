using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxn;
    internal Dictionary<KeyAttr, Operation> Rset;
    internal Dictionary<KeyAttr, Operation> Wset;
    public ManualResetEvent mre = new ManualResetEvent(false);

    public TransactionContext(){

    }
    public void Init(int startTxn){
        this.startTxn = startTxn;
        mre.Reset();
        status = TransactionStatus.Idle;
        Rset = new Dictionary<KeyAttr, Operation>(new OCCComparer());
        Wset = new Dictionary<KeyAttr, Operation>(new OCCComparer());
    }

    public byte[]? GetFromContext(KeyAttr keyAttr){
        byte[]? val = null;
        if (Wset.ContainsKey(keyAttr)){
            val = Wset[keyAttr].Value;
        } else if (Rset.ContainsKey(keyAttr)){
            val = Rset[keyAttr].Value;
        }
        if (val != null) {
            SetInContext(OperationType.Read, keyAttr, val);
        }
        return val;
    }

    public void SetInContext(OperationType op, KeyAttr keyAttr, ReadOnlySpan<byte> val){
        if (op == OperationType.Read) {
            Rset[keyAttr] = new Operation(op, new TupleId(keyAttr.Key, keyAttr.Table.GetHashCode()), new TupleDesc[]{new TupleDesc(keyAttr.Attr, val.Length)}, val);
        } else {
            Wset[keyAttr] = new Operation(op, new TupleId(keyAttr.Key, keyAttr.Table.GetHashCode()), new TupleDesc[]{new TupleDesc(keyAttr.Attr, val.Length)}, val);
        }
    }

    public Dictionary<KeyAttr, Operation> GetReadset(){
        return Rset;
    }
    public Dictionary<KeyAttr, Operation> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}