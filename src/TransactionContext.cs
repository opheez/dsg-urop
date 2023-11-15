using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal List<(KeyAttr, byte[])> Rset;
    internal List<(KeyAttr, byte[])> Wset;
    public long tid;

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new List<(KeyAttr, byte[])>();
        Wset = new List<(KeyAttr, byte[])>();
    }

    public int GetWriteSetKeyIndex(KeyAttr keyAttr){
        for (int i = Wset.Count-1; i >= 0; i--){
            if (Wset[i].Item1.Key == keyAttr.Key && Wset[i].Item1.Table.GetHashCode() == keyAttr.Table.GetHashCode()){
                return i;
            }
        }
        return -1;
    }

    public int GetReadsetKeyIndex(KeyAttr keyAttr){
        for (int i = Rset.Count-1; i >= 0; i--){
            if (Rset[i].Item1.Key == keyAttr.Key && Rset[i].Item1.Table.GetHashCode() == keyAttr.Table.GetHashCode()){
                return i;
            }
        }
        return -1;
    }
    private int GetWriteSetKeyAttrIndex(KeyAttr keyAttr){
        for (int i = Wset.Count-1; i >= 0; i--){
            if (Wset[i].Item1.Key == keyAttr.Key && Wset[i].Item1.Attr == keyAttr.Attr && Wset[i].Item1.Table.GetHashCode() == keyAttr.Table.GetHashCode()){
                return i;
            }
        }
        return -1;
    }

    private int GetReadsetKeyAttrIndex(KeyAttr keyAttr){
        for (int i = Rset.Count-1; i >= 0; i--){
            if (Rset[i].Item1.Key == keyAttr.Key && Rset[i].Item1.Attr == keyAttr.Attr && Rset[i].Item1.Table.GetHashCode() == keyAttr.Table.GetHashCode()){
                return i;
            }
        }
        return -1;
    }

    public byte[]? GetFromContext(KeyAttr keyAttr){
        byte[]? val = null;
        int wi = GetWriteSetKeyAttrIndex(keyAttr);
        if (wi != -1){
            val = Wset[wi].Item2;
        } else {
            int ri = GetReadsetKeyAttrIndex(keyAttr);
            if (ri != -1){
                val = Rset[ri].Item2;
            }
        }
        if (val != null) {
            SetInContext(OperationType.Read, keyAttr, val);
        }
        return val;
    }

    public void SetInContext(OperationType op, KeyAttr keyAttr, ReadOnlySpan<byte> val){
        if (op == OperationType.Read) {
            Rset.Add((keyAttr, val.ToArray()));
        } else {
            Wset.Add((keyAttr, val.ToArray()));
        }
    }

    public List<(KeyAttr, byte[])> GetReadset(){
        return Rset;
    }
    public List<(KeyAttr, byte[])> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}