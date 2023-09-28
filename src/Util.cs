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
        public Operation(OperationType type, KeyAttr keyAttr, byte[]? val){
            if (type != OperationType.Read && val == null) {
                throw new ArgumentException("Writes must provide a non-null value");
            }
            Type = type;
            Value = val;
            KeyAttribute = keyAttr;
        }
        public OperationType Type;
        public byte[]? Value;
        public KeyAttr KeyAttribute;

    }

    public struct KeyAttr{ //} : IEquatable<KeyAttr>{

        public KeyAttr(long key, long? attr, Table t){
            Key = key;
            Attr = attr;
            Table = t;
        }
        public long Key;
        public long? Attr;
        public Table Table;

        public override string ToString(){
            return $"({Key}, {Attr})";
        }

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
    }

}