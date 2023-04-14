
/// <summary>
/// 
/// </summary>
public class TransactionContext<Key, Value> {

    public enum Status {
        Idle,
        Pending,
        Committed
    }

    public struct KeyAttr{
        Key key;
        Key attr;
    }

    internal Dictionary<KeyAttr, Value> ReadSet;
    internal Dictionary<KeyAttr, Value> WriteSet;

    public Value? ReadContext(){
        
    }
    public Value? UpsertContext(){

    }
}