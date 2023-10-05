using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxn;
    internal Dictionary<KeyAttr, byte[]> Rset;
    internal Dictionary<KeyAttr, byte[]> Wset;
    public ManualResetEvent mre = new ManualResetEvent(false);

    public TransactionContext(){

    }
    public void Init(int startTxn){
        this.startTxn = startTxn;
        mre.Reset();
        status = TransactionStatus.Idle;
        Rset = new Dictionary<KeyAttr, byte[]>(new OCCComparer());
        Wset = new Dictionary<KeyAttr, byte[]>(new OCCComparer());
    }

    public byte[]? GetFromContext(KeyAttr keyAttr){
        byte[]? val = null;
        if (Wset.ContainsKey(keyAttr)){
            val = Wset[keyAttr];
        } else if (Rset.ContainsKey(keyAttr)){
            val = Rset[keyAttr];
        }
        if (val != null) {
            SetInContext(OperationType.Read, keyAttr, val);
        }
        return val;
    }

    public void SetInContext(OperationType op, KeyAttr keyAttr, ReadOnlySpan<byte> val){
        if (op == OperationType.Read) {
            Rset[keyAttr] = val.ToArray();
        } else {
            Wset[keyAttr] =val.ToArray();
        }
    }

    public Dictionary<KeyAttr, byte[]> GetReadset(){
        return Rset;
    }
    public Dictionary<KeyAttr, byte[]> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}