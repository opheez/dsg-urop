using System.Runtime.InteropServices;
using System.Text;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal Dictionary<KeyAttr, byte[]> Rset;
    internal Dictionary<TupleId, Dictionary<TupleDesc, byte[]>> Wset;
    public long tid;

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new Dictionary<KeyAttr, byte[]>();
        Wset = new Dictionary<TupleId, Dictionary<TupleDesc, byte[]>>();
    }

    public bool InReadSet(TupleId tupleId){
        foreach (KeyAttr ka in Rset.Keys){
            if (ka.Key == tupleId.Key && ka.Table == tupleId.Table){
                return true;
            }
        }
        return false;
    }

    public bool InWriteSet(TupleId tupleId){
        return Wset.ContainsKey(tupleId);
    }

    public ReadOnlySpan<byte> GetFromContext(KeyAttr keyAttr){
        ReadOnlySpan<byte> val = null;
        Dictionary<TupleDesc, byte[]> wsetVal = Wset.GetValueOrDefault(new TupleId(keyAttr.Key, keyAttr.Table), null);
        if (wsetVal != null) {
            TupleDesc td = new TupleDesc(keyAttr.Attr, keyAttr.Table.metadata[keyAttr.Attr].Item1);
            val = wsetVal.GetValueOrDefault(td, null);
        }
        // (int size, int offset) = keyAttr.Table.metadata[keyAttr.Attr];
        if (val == null){
            val = Rset.GetValueOrDefault(keyAttr, null);
        }
        if (val != null) {
            AddReadSet(keyAttr, val);
        }
        return val;
    }

    public void AddReadSet(KeyAttr keyAttr, ReadOnlySpan<byte> val){
        Rset[keyAttr] = val.ToArray();
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

    public Dictionary<KeyAttr, byte[]> GetReadset(){
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