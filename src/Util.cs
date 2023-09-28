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

    }

    public struct TupleId{ //} : IEquatable<TupleId>{

        public TupleId(long key, Table t){
            Key = key;
            Table = t;
        }
        public long Key;
        public Table Table;

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

    public class OCCComparer : IEqualityComparer<TupleId>
    {
        public bool Equals(TupleId x, TupleId y)
        {
            return x.Key == y.Key && x.Table == y.Table;
        }

        public int GetHashCode(TupleId obj)
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
    }

}