using System.Runtime.InteropServices;
using System.Text;

namespace DB {
/// <summary>
/// Data structure holding transaction context
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal List<(PrimaryKey, byte[])> Rset = new(50); // byte[] is the entire record
    internal List<(TupleDesc[], byte[])> Wset = new(50); // byte[] corresponds to the TupleDesc
    internal List<PrimaryKey> WsetKeys = new(50); // byte[] corresponds to the TupleDesc
    public long tid;
    public Dictionary<int, Table> tables;
    public Action<bool> callback;
    public TransactionContext(Dictionary<int, Table> tables){
        this.tables = tables;
    }

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new List<(PrimaryKey, byte[])>();
        Wset = new List<(TupleDesc[], byte[])>();
        WsetKeys = new List<PrimaryKey>();
    }

    public bool InReadSet(ref PrimaryKey tupleId){
        return GetReadsetKeyIndex(ref tupleId) != -1;
    }
    public bool InWriteSet(ref PrimaryKey tupleId){
        return GetWriteSetKeyIndex(ref tupleId) != -1;
    }

    public (TupleDesc[], byte[]) GetFromWriteset(PrimaryKey tupleId){
        int index = GetWriteSetKeyIndex(ref tupleId);
        if (index == -1){
            return (null, null);
        }
        return (Wset[index].Item1, Wset[index].Item2);
    }
    public (TupleDesc[], byte[]) GetFromWriteset(int i){
        return (Wset[i].Item1, Wset[i].Item2);
    }

    public ReadOnlySpan<byte> GetFromReadset(PrimaryKey tupleId){
        int index = GetReadsetKeyIndex(ref tupleId);
        if (index == -1){
            return null;
        }
        return Rset[index].Item2;
    }

    public void AddReadSet(PrimaryKey tupleId, ReadOnlySpan<byte> val){
        // TODO: varlen
        if (val.Length != tables[tupleId.Table].rowSize){
            throw new ArgumentException($"Readset value length {val.Length} does not match table row size {tables[tupleId.Table].rowSize}");
        }
        Rset.Add((tupleId,val.ToArray()));
    }

    public void AddWriteSet(ref PrimaryKey tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        int index = GetWriteSetKeyIndex(ref tupleId);
        if (index != -1){
            (TupleDesc[], byte[]) existing = Wset[index];
            // List<byte> result = new List<byte>();
            // int start = 0;
            // foreach (TupleDesc td in existing.Item2){
            //     bool included = false;
            //     int newStart = 0;
            //     foreach (TupleDesc newTd in tupleDescs){
            //         if (td.Attr == newTd.Attr){
            //             included = true;

            //             result.AddRange(val.Slice(newStart, td.Size));
            //         }
            //         newStart += newTd.Size;
            //     }
            //     if (!included) finalSize += td.Size;


            //     if (td.Attr == tupleDescs[0].Attr){
            //         result.AddRange(val.ToArray());
            //     } else {
            //         result.AddRange(existing.Item3.AsSpan(start, td.Size).ToArray());
            //     }
            //     start += td.Size;
            // }

            // calculate final size
            int finalSize = existing.Item2.Length;
            foreach (TupleDesc td in tupleDescs){
                bool included = false;
                foreach (TupleDesc existingTd in existing.Item1){
                    if (td.Attr == existingTd.Attr){
                        included = true;
                    }
                }
                if (!included) finalSize += td.Size;
            }

            // copy values, replacing existing values with new ones
            Span<byte> newVal = new byte[finalSize];
            bool[] includedTd = new bool[tupleDescs.Length];
            foreach (TupleDesc existingTd in existing.Item1){
                bool included = false;
                for (int i = 0; i < tupleDescs.Length; i++){
                    TupleDesc newTd = tupleDescs[i];
                    if (existingTd.Attr == newTd.Attr){
                        included = true;
                        includedTd[i] = true;
                        val.Slice(newTd.Offset, newTd.Size).CopyTo(newVal.Slice(existingTd.Offset, newTd.Size));
                        break;
                    }
                }
                if (!included) {
                    existing.Item2.AsSpan(existingTd.Offset, existingTd.Size).CopyTo(newVal.Slice(existingTd.Offset, existingTd.Size));
                } 
            }

            // add remaining values, also to tupleDescs
            TupleDesc[] newTupleDescs = new TupleDesc[existing.Item1.Length + includedTd.Count(x => !x)];
            existing.Item1.CopyTo(newTupleDescs, 0);
            int start = existing.Item2.Length;
            int j = existing.Item1.Length;
            for (int i = 0; i < tupleDescs.Length; i++){
                if (!includedTd[i]){
                    val.Slice(tupleDescs[i].Offset, tupleDescs[i].Size).CopyTo(newVal.Slice(start, tupleDescs[i].Size));
                    newTupleDescs[j++] = tupleDescs[i];
                    start += tupleDescs[i].Size;
                }
            }

            Wset.Add((newTupleDescs, newVal.ToArray()));
            WsetKeys.Add(tupleId);
        } else {
            Wset.Add((tupleDescs, val.ToArray()));
            WsetKeys.Add(tupleId);
        }
    }

    public List<(PrimaryKey, byte[])> GetReadset(){
        return Rset;
    }
    public List<PrimaryKey>GetWritesetKeys(){
        return WsetKeys;
    }

    private int GetWriteSetKeyIndex(ref PrimaryKey tupleId){
        var span = CollectionsMarshal.AsSpan(WsetKeys);
        for (int i = span.Length-1; i >= 0; i--){
            ref PrimaryKey pk = ref span[i];
            // TupleDesc[] tupleDescs = span[i].Item1;
            // if (tupleDescs[0].Attr == -1){
            //     return i;
            // }
            // if (pk.Equals(tupleId)){
            //     return i;
            // }
            if (pk.Table == tupleId.Table && pk.Key1 == tupleId.Key1
                && pk.Key2 == tupleId.Key2 && pk.Key3 == tupleId.Key3
                && pk.Key4 == tupleId.Key4 && pk.Key5 == tupleId.Key5
                && pk.Key6 == tupleId.Key6 
            ){
                return i;
            }
        }
        return -1;
    }
    
    private int GetReadsetKeyIndex(ref PrimaryKey tupleId){
        for (int i = Rset.Count-1; i >= 0; i--){
            if (Rset[i].Item1.Equals(tupleId)){
                return i;
            }
        }
        return -1;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWritesetKeys())}";
    }
}

}