using System;
using System.Drawing;
using System.Runtime.InteropServices;

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

    // public struct Operation {
    //     public Operation(OperationType type, TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
    //         if (type != OperationType.Read && Util.IsEmpty(val)) {
    //             throw new ArgumentException("Writes must provide a non-null value");
    //         }
    //         Type = type;
    //         TupleID = tupleId;
    //         TupleDescs = tupleDescs;
    //         Value = val.ToArray();
    //     }
    //     public OperationType Type;
    //     public TupleId TupleID;
    //     public TupleDesc[] TupleDescs;
    //     public byte[] Value;


    //     public byte[] ToBytes(){
    //         using (MemoryStream m = new MemoryStream()) {
    //             using (BinaryWriter writer = new BinaryWriter(m)) {
    //                 writer.Write(BitConverter.GetBytes((int)Type));
    //                 writer.Write(TupleID.Key);
    //                 writer.Write(TupleID.TableHash);
    //                 writer.Write(TupleDescs.Count());
    //                 foreach (TupleDesc desc in TupleDescs){
    //                     writer.Write(desc.Attr);
    //                     writer.Write(desc.Size);
    //                 }
    //                 writer.Write(Value);
    //             }
    //             return m.ToArray();
    //         }
    //     }

    //     public static Operation FromBytes(byte[] data) {
    //         Operation op = new Operation();
    //         using (MemoryStream m = new MemoryStream(data)) {
    //             using (BinaryReader reader = new BinaryReader(m)) {
    //                 op.Type = (OperationType)reader.ReadInt32();

    //                 long key = reader.ReadInt64();
    //                 int tableHash = reader.ReadInt32();
    //                 op.TupleID = new TupleId(key, tableHash);

    //                 int tupleDescLength = reader.ReadInt32();
    //                 TupleDesc[] descs = new TupleDesc[tupleDescLength];
    //                 for (int i = 0; i < tupleDescLength; i++){
    //                     TupleDesc desc = new TupleDesc();
    //                     desc.Attr = reader.ReadInt64();
    //                     desc.Size = reader.ReadInt32();
    //                     descs[i] = desc;
    //                 }
    //             }
    //         }
    //         return op;
    //     }

    // }

    public struct PrimaryKey{ //} : IEquatable<TupleId>{

        public readonly long[] Keys;
        public readonly int Table;
        public PrimaryKey(int t, params long[] keys){
            Keys = keys;
            Table = t;
        }

        public int Size => sizeof(long) * Keys.Length + sizeof(int) + sizeof(int);

        public override bool Equals(object o){
            if (o == null || GetType() != o.GetType()){
                return false;
            }
            for (int i = 0; i < Keys.Length; i++){
                if (Keys[i] != ((PrimaryKey)o).Keys[i]){
                    return false;
                }
            }
            return Table == ((PrimaryKey)o).Table;
        }

        public override int GetHashCode(){
            int hash = 17;
            foreach (long l in Keys){
                hash = hash * 31 + l.GetHashCode();
            }
            return hash * 31 + Table;
        }

        public override string ToString(){
            return $"PK ({string.Join(", ", Keys)}) Table {Table}";
        }

        public unsafe byte[] ToBytes(){
            byte[] arr = new byte[Size];
            
            fixed (byte* b = arr) {
                var head = b;
                *(int*)head = Keys.Length;
                head += sizeof(int);
                for (int i = 0; i < Keys.Length; i++){
                    *(long*)head = Keys[i];
                    head += sizeof(long);
                }
                *(int*)head = Table;
            }
            return arr;
        }

        public static unsafe PrimaryKey FromBytes(byte[] data){
            PrimaryKey result;
            fixed (byte* b = data) {
                var head = b;
                int len = *(int*)head;
                head += sizeof(int);
                long[] keys = new long[len];
                for (int i = 0; i < keys.Length; i++){
                    keys[i] = *(long*)head;
                    head += sizeof(long);
                }
                result = new PrimaryKey(*(int*)head, keys);
            }
            return result;
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
        public TupleDesc(long attr, int size, int offset){
            Attr = attr;
            Size = size;
            Offset = offset;
        }
        public long Attr;
        public int Size;
        public int Offset;

        public override string ToString(){
            return $"(Attr:{Attr}, Size:{Size}, Offset:{Offset})";
        }
    }

    public struct KeyAttr {
        public KeyAttr(PrimaryKey key, long attr){
            Key = key;
            Attr = attr;
        }
        public PrimaryKey Key;
        public long Attr;
        public int Size => Key.Size + sizeof(long);

        public override string ToString(){
            return $"KA ({Key}, {Attr})";
        }

        public unsafe byte[] ToBytes(){
            byte[] arr = new byte[Size];
            fixed (byte* b = arr) {
                var head = b;
                Key.ToBytes().CopyTo(new Span<byte>(head, Key.Size));
                head += Key.Size;
                *(long*)head = Attr;
            }
            return arr;
        }

        public static unsafe KeyAttr FromBytes(byte[] data) {
            KeyAttr result = new KeyAttr();

            fixed (byte* b = data) {
                var head = b;
                result.Key = PrimaryKey.FromBytes(new Span<byte>(head, data.Length - sizeof(long)).ToArray());
                head += result.Key.Size;
                result.Attr = *(long*)head;
            }
            return result;
        }
    }

    public class OCCComparer : IEqualityComparer<KeyAttr>
    {
        public bool Equals(KeyAttr x, KeyAttr y)
        {
            return x.Key.Equals(y.Key);
        }

        public int GetHashCode(KeyAttr obj)
        {
            return obj.GetHashCode();
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

        public static int GetLength(byte[][] arr){
            int len = 0;
            foreach (byte[] a in arr){
                len += a.Length;
            }
            return len;
        }

        public static int CompareArrays<T>(IEnumerable<T> first, IEnumerable<T> second) where T : IComparable<T>
        {
            using (var firstEnum = first.GetEnumerator())
            using (var secondEnum = second.GetEnumerator())
            {
                while (firstEnum.MoveNext())
                {
                    if (!secondEnum.MoveNext())
                        return 1;

                    int cmp = firstEnum.Current.CompareTo(secondEnum.Current);
                    if (cmp != 0)
                        return cmp;
                }

                return secondEnum.MoveNext() ? -1 : 0;
            }
        }
    }

}