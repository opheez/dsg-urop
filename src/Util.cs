
namespace DB {

    public enum TransactionStatus {
        Idle,
        Pending,
        Committed,
        Aborted
    }
    public struct KeyAttr{ //} : IEquatable<KeyAttr>{

        public KeyAttr(long key, long attr, Table t){
            Key = key;
            Attr = attr;
            Table = t;
        }
        public long Key;
        public long Attr;
        public Table Table;

        // public bool Equals(KeyAttr o){
        //     return Key == o.Key && Attr == o.Attr && Table == o.Table;
        // }

        // public override bool Equals([NotNullWhen(true)] object o)
        // {
        //     if (o == null || GetType() != o.GetType())
        //     {
        //         return false;
        //     }
        //     return Equals((KeyAttr)o);
        // }

        // public override int GetHashCode(){
        //     return (int)Key + (int)Attr + Table.GetHashCode();
        // }
        
    }


}