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

    public TransactionContext(uint startTxn){
        this.startTxn = startTxn;
        status = TransactionStatus.Idle;
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