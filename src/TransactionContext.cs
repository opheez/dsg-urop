using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, used for a single table
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal uint startTxn;
    internal Dictionary<KeyAttr, byte[]?> Rset;
    internal Dictionary<KeyAttr, byte[]?> Wset;
    //** Tianyu: Not used?
    public object l;

    //** Tianyu: I would highly recommend having a Reset() function here as we likely want to keep a pool of
    //** TransactionContext objects for replay and avoid allocating new ones frequently
    public TransactionContext(uint startTxn){
        this.startTxn = startTxn;
        status = TransactionStatus.Idle;
        //** Tianyu: I am pretty sure OCC works on a tuple level rather than an attribute level? i.e., two writes
        //** overlap even if they update disjoint attributes in a tuple. 
        Rset = new Dictionary<KeyAttr, byte[]?>();
        Wset = new Dictionary<KeyAttr, byte[]?>();
    }

    public byte[]? GetFromContext(KeyAttr keyAttr){
        byte[]? val = null;
        if (Wset.ContainsKey(keyAttr)){
            val = Wset[keyAttr];
        } else if (Rset.ContainsKey(keyAttr)){
            val = Rset[keyAttr];
        }
        SetInContext(keyAttr, val, false);
        return val;
    }

    public void SetInContext(KeyAttr keyAttr, ReadOnlySpan<byte> val){
        SetInContext(keyAttr, val, true);
    }
    private void SetInContext(KeyAttr keyAttr, ReadOnlySpan<byte> val, bool write){
        if (write){
            Wset[keyAttr] = val.ToArray();
        } else {
            Rset[keyAttr] = val.ToArray();
        }
    }

    public Dictionary<KeyAttr, byte[]?> GetReadset(){
        return Rset;
    }
    public Dictionary<KeyAttr, byte[]?> GetWriteset(){
        return Wset;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadset())}\nWriteset: {string.Join(Environment.NewLine, GetWriteset())}";
    }
}

}