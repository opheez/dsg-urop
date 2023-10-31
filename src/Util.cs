using System;
namespace DB {

    public enum TransactionStatus {
        Idle,
        Pending,
        Validated,
        Committed,
        Aborted
    }

    public enum OperationType {
        Read,
        Insert,
        Update
    }

    public unsafe struct Pointer {
        public Pointer(IntPtr ptr, int size){
            Size = size;
            IntPointer = ptr;
            Ptr = ptr.ToPointer();
        }
        public Pointer(void* ptr, int size){
            Size = size;
            IntPointer = new IntPtr(ptr);
            Ptr = ptr;
        }
        public int Size;
        public IntPtr IntPointer;
        public void* Ptr;
    }

    public struct Operation {
        public Operation(OperationType type, TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
            if (type != OperationType.Read && Util.IsEmpty(val)) {
                throw new ArgumentException("Writes must provide a non-null value");
            }
            Type = type;
            TupleID = tupleId;
            TupleDescs = tupleDescs;
            Value = val.ToArray();
        }
        public OperationType Type;
        public TupleId TupleID;
        public TupleDesc[] TupleDescs;
        public byte[] Value;


        public byte[] ToBytes(){
            using (MemoryStream m = new MemoryStream()) {
                using (BinaryWriter writer = new BinaryWriter(m)) {
                    writer.Write(BitConverter.GetBytes((int)Type));
                    writer.Write(TupleID.Key);
                    writer.Write(TupleID.TableHash);
                    writer.Write(TupleDescs.Count());
                    foreach (TupleDesc desc in TupleDescs){
                        writer.Write(desc.Attr);
                        writer.Write(desc.Size);
                    }
                    writer.Write(Value);
                }
                return m.ToArray();
            }
        }

        public static Operation FromBytes(byte[] data) {
            Operation op = new Operation();
            using (MemoryStream m = new MemoryStream(data)) {
                using (BinaryReader reader = new BinaryReader(m)) {
                    op.Type = (OperationType)reader.ReadInt32();

                    long key = reader.ReadInt64();
                    int tableHash = reader.ReadInt32();
                    op.TupleID = new TupleId(key, tableHash);

                    int tupleDescLength = reader.ReadInt32();
                    TupleDesc[] descs = new TupleDesc[tupleDescLength];
                    for (int i = 0; i < tupleDescLength; i++){
                        TupleDesc desc = new TupleDesc();
                        desc.Attr = reader.ReadInt64();
                        desc.Size = reader.ReadInt32();
                        descs[i] = desc;
                    }
                }
            }
            return op;
        }

    }

    public struct TupleId{ //} : IEquatable<TupleId>{

        public TupleId(long key, int tHash){
            Key = key;
            TableHash = tHash;
        }
        public long Key;
        public int TableHash;

        public override string ToString(){
            return $"({Key})";
        }

        // public bool Equals(TupleId o){
        //     return Key == o.Key && Attr == o.Attr && Table == o.Table;
        // }

        // public override bool Equals([NotNullWhen(true)] object o)
        // {
        //     if (o == null || GetType() != o.GetType())
        //     {
        //         return false;
        //     }
        //     return Equals((TupleId)o);
        // }

        // public override int GetHashCode(){
        //     return (int)Key + (int)Attr + Table.GetHashCode();
        // }
        
    }

    public struct TupleDesc {
        public TupleDesc(long attr, int size){
            Attr = attr;
            Size = size;
        }
        public long Attr;
        public int Size;
    }

    public struct KeyAttr {
        public KeyAttr(long key, long attr, Table t){
            Key = key;
            Attr = attr;
            Table = t;
        }
        public long Key;
        public long Attr;
        public Table Table;

        public override string ToString(){
            return $"({Key}, {Attr})";
        }
    }

    public class OCCComparer : IEqualityComparer<KeyAttr>
    {
        public bool Equals(KeyAttr x, KeyAttr y)
        {
            return x.Key == y.Key && x.Table == y.Table;
        }

        public int GetHashCode(KeyAttr obj)
        {
            return (int)obj.Key + obj.Table.GetHashCode(); //Already an int
        }
    }

    public class Util {
        public static bool IsEmpty(ReadOnlySpan<byte> val){
            if (val.IsEmpty) {
                return true;
            }
            foreach (byte b in val)
            {
                if (b != 0)
                {
                    return false; // If any element is not 0, return false
                }
            }
            return true; // All elements are 0
        }

        public static bool IsTerminalStatus(TransactionStatus status){
            return status == TransactionStatus.Committed || status == TransactionStatus.Aborted;
        }
    }

}