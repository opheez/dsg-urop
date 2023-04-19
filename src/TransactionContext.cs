using System.Runtime.InteropServices;

namespace DB {
/// <summary>
/// Data structure holding transaction context, should be used for a single table but not tied to one
/// </summary>
public class TransactionContext {

    internal TransactionStatus status;
    internal Dictionary<KeyAttr, (bool, byte[]?)> RWset;

    public TransactionContext(){
        status = TransactionStatus.Idle;
        RWset = new Dictionary<KeyAttr, (bool, byte[]?)>();
    }

    public byte[]? GetFromContext(KeyAttr keyAttr){
        if (RWset.ContainsKey(keyAttr)){
            return RWset[keyAttr].Item2;
        }
        return null;
    }

    public void SetInContext(KeyAttr keyAttr, Span<byte> val, bool write){
        RWset[keyAttr] = (write, val.ToArray());
    }

    public Dictionary<KeyAttr, byte[]> GetReadset(){
        return RWset.Where(item => !item.Value.Item1).ToDictionary(item => item.Key, item => item.Value.Item2);
    }
    public Dictionary<KeyAttr, byte[]> GetWriteset(){
        return RWset.Where(item => item.Value.Item1).ToDictionary(item => item.Key, item => item.Value.Item2);
    }
}

}