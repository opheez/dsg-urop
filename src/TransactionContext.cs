using System.Runtime.InteropServices;
using System.Text;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal int startTxnNum;
    internal List<(TupleId, byte[])> Rset = new(); // byte[] is the entire record
    internal List<(TupleId, TupleDesc[], byte[])> Wset = new(); // byte[] corresponds to the TupleDesc
    public long tid;

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new List<(TupleId, byte[])>();
        Wset = new List<(TupleId, TupleDesc[], byte[])>();
    }

    public bool InReadSet(TupleId tupleId){
        return GetReadsetKeyIndex(tupleId) != -1;
    }
    public bool InWriteSet(TupleId tupleId){
        return GetWriteSetKeyIndex(tupleId) != -1;
    }

    public (TupleDesc[], byte[]) GetFromWriteset(TupleId tupleId){
        int index = GetWriteSetKeyIndex(tupleId);
        if (index == -1){
            return (null, null);
        }
        return (Wset[index].Item2, Wset[index].Item3);
    }

    public ReadOnlySpan<byte> GetFromReadset(TupleId tupleId){
        int index = GetReadsetKeyIndex(tupleId);
        if (index == -1){
            return null;
        }
        return Rset[index].Item2;
    }

    public void AddReadSet(TupleId tupleId, ReadOnlySpan<byte> val){
        // TODO: varlen
        if (val.Length != tupleId.Table.rowSize){
            throw new ArgumentException($"Readset value length {val.Length} does not match table row size {tupleId.Table.rowSize}");
        }
        Rset.Add((tupleId,val.ToArray()));
    }

    public void AddWriteSet(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        int index = GetWriteSetKeyIndex(tupleId);
        if (index != -1){
            (TupleId, TupleDesc[], byte[]) existing = Wset[index];
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
            int finalSize = existing.Item3.Length;
            foreach (TupleDesc td in tupleDescs){
                bool included = false;
                foreach (TupleDesc existingTd in existing.Item2){
                    if (td.Attr == existingTd.Attr){
                        included = true;
                    }
                }
                if (!included) finalSize += td.Size;
            }

            // copy values, replacing existing values with new ones
            Span<byte> newVal = new byte[finalSize];
            bool[] includedTd = new bool[tupleDescs.Length];
            foreach (TupleDesc existingTd in existing.Item2){
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
                    existing.Item3.AsSpan(existingTd.Offset, existingTd.Size).CopyTo(newVal.Slice(existingTd.Offset, existingTd.Size));
                } 
            }

            // add remaining values, also to tupleDescs
            TupleDesc[] newTupleDescs = new TupleDesc[existing.Item2.Length + includedTd.Count(x => !x)];
            existing.Item2.CopyTo(newTupleDescs, 0);
            int start = existing.Item3.Length;
            int j = existing.Item2.Length;
            for (int i = 0; i < tupleDescs.Length; i++){
                if (!includedTd[i]){
                    val.Slice(tupleDescs[i].Offset, tupleDescs[i].Size).CopyTo(newVal.Slice(start, tupleDescs[i].Size));
                    newTupleDescs[j++] = tupleDescs[i];
                    start += tupleDescs[i].Size;
                }
            }

            Wset.Add((tupleId, newTupleDescs, newVal.ToArray()));
        } else {
            Wset.Add((tupleId, tupleDescs, val.ToArray()));
        }
    }

    public List<(TupleId, byte[])> GetReadset(){
        return Rset;
    }
    public List<(TupleId, TupleDesc[], byte[])>GetWriteset(){
        return Wset;
    }

    private int GetWriteSetKeyIndex(TupleId tupleId){
        for (int i = Wset.Count-1; i >= 0; i--){
            if (Wset[i].Item1.Equals(tupleId)){
                return i;
            }
        }
        return -1;
    }
    
    private int GetReadsetKeyIndex(TupleId tupleId){
        for (int i = Rset.Count-1; i >= 0; i--){
            if (Rset[i].Item1.Equals(tupleId)){
                return i;
            }
        }
        return -1;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}