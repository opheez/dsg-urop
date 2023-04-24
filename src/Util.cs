
namespace DB {

    public enum TransactionStatus {
        Idle,
        Pending,
        Committed,
        Aborted
    }
    public struct KeyAttr{

        public KeyAttr(long key, long attr, Table t){
            Key = key;
            Attr = attr;
            Table = t;
        }
        public long Key;
        public long Attr;
        public Table Table;
    }


}