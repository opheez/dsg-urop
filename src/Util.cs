
namespace DB {

    public enum TransactionStatus {
        Idle,
        Pending,
        Committed,
        Aborted
    }
    public struct KeyAttr{

        public KeyAttr(long key, long attr){
            Key = key;
            Attr = attr;
        }
        public long Key;
        public long Attr;
    }


}