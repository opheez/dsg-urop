using System.Runtime.InteropServices;
using System.Text;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal List<(KeyAttr, byte[])> Rset;
    internal List<(TupleId, TupleDesc[], byte[])> Wset;
    public long tid;

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new List<(KeyAttr, byte[])>();
        Wset = new List<(TupleId, TupleDesc[], byte[])>();
    }

    public int GetWriteSetKeyIndex(TupleId tupleId){
        for (int i = Wset.Count-1; i >= 0; i--){
            if (Wset[i].Item1.Equals(tupleId)){
                return i;
            }
        }
        return -1;
    }

    public int GetReadsetKeyIndex(TupleId tupleId){
        for (int i = Rset.Count-1; i >= 0; i--){
            if (Rset[i].Item1.Equals(tupleId)){
                return i;
            }
        }
        return -1;
    }
    private byte[]? GetWriteSetKeyAttr(KeyAttr keyAttr){
        for (int i = Wset.Count-1; i >= 0; i--){
            if (Wset[i].Item1.Key == keyAttr.Key && Wset[i].Item1.Table.GetHashCode() == keyAttr.Table.GetHashCode()){
                int start = 0;
                for (int j = 0; j < Wset[i].Item2.Length; j++){
                    int size = Wset[i].Item2[j].Size;
                    if (Wset[i].Item2[j].Attr == keyAttr.Attr){
                        return Wset[i].Item3.AsSpan(start, size).ToArray();
                    }
                    start += size;
                }
            }
        }
        return null;
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
        byte[]? val = GetWriteSetKeyAttr(keyAttr);
        // (int size, int offset) = keyAttr.Table.metadata[keyAttr.Attr];
        if (val == null){
            int ri = GetReadsetKeyAttrIndex(keyAttr);
            if (ri != -1){
                val = Rset[ri].Item2;
            }
        }
        if (val != null) {
            AddReadSet(keyAttr, val);
        }
        return val;
    }

    public void AddReadSet(KeyAttr keyAttr, ReadOnlySpan<byte> val){
        Rset.Add((keyAttr, val.ToArray()));
    }

    public void AddWriteSet(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        Wset.Add((tupleId, tupleDescs, val.ToArray()));
    }

    public List<(KeyAttr, byte[])> GetReadset(){
        return Rset;
    }
    public List<(TupleId, TupleDesc[], byte[])> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}