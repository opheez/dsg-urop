using System.Runtime.InteropServices;
using System.Text;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal Dictionary<TupleId, byte[]> Rset = new(); // byte[] is the entire record
    internal Dictionary<TupleId, Dictionary<TupleDesc, byte[]>> Wset = new(); // byte[] corresponds to the TupleDesc
    public long tid;

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new Dictionary<TupleId, byte[]>();
        Wset = new Dictionary<TupleId, Dictionary<TupleDesc, byte[]>>();
    }

    public bool InReadSet(TupleId tupleId){
        return Rset.ContainsKey(tupleId);
    }
    public bool InWriteSet(TupleId tupleId){
        return Wset.ContainsKey(tupleId);
    }

    // public ReadOnlySpan<byte> GetFromContext(TupleId tupleId){
    //     ReadOnlySpan<byte> val = null;
    //     Dictionary<TupleDesc, byte[]> wsetVal = Wset.GetValueOrDefault(tupleId, null);
    //     if (wsetVal != null) {
    //         TupleDesc td = new TupleDesc(keyAttr.Attr, keyAttr.Table.metadata[keyAttr.Attr].Item1);
    //         val = wsetVal.GetValueOrDefault(td, null);
    //     }

    //     if (val == null){
    //         val = Rset.GetValueOrDefault(tupleId, null);
    //     }
    //     if (val != null) {
    //         AddReadSet(tupleId, val);
    //     }
    //     return val;
    // }

    // public int GetReadsetKeyIndex(TupleId tupleId){
    //     for (int i = Rset.Count-1; i >= 0; i--){
    //         if (Rset[i].Item1.Equals(tupleId)){
    //             return i;
    //         }
    //     }
    //     return -1;
    // }
    public Dictionary<TupleDesc, byte[]> GetFromWriteset(TupleId tupleId){
       return Wset.GetValueOrDefault(tupleId, null);
    }

    public ReadOnlySpan<byte> GetFromReadset(TupleId tupleId){
        return Rset.GetValueOrDefault(tupleId, null);
    }

    public void AddReadSet(TupleId tupleId, ReadOnlySpan<byte> val){
        // TODO: varlen
        if (val.Length != tupleId.Table.rowSize){
            throw new ArgumentException($"Readset value length {val.Length} does not match table row size {tupleId.Table.rowSize}");
        }
        Rset[tupleId] = val.ToArray();
    }

    public void AddWriteSet(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        Dictionary<TupleDesc, byte[]> wsetVal = Wset.GetValueOrDefault(tupleId, new Dictionary<TupleDesc, byte[]>());

        int start = 0;
        foreach (TupleDesc td in tupleDescs){
            wsetVal[td] = val.Slice(start, td.Size).ToArray(); // TODO: dont use ToArray();
            start += td.Size;
        }

        Wset[tupleId] = wsetVal;
    }

    public Dictionary<TupleId, byte[]> GetReadset(){
        return Rset;
    }
    public Dictionary<TupleId, Dictionary<TupleDesc, byte[]>> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }

    private string PrintDictionary(Dictionary<TupleDesc, byte[]> dict){
        StringBuilder sb = new StringBuilder();
        foreach (var item in dict){
            sb.Append($"({item.Key}, {item.Value})");
        }
        return sb.ToString();
    }
}

}