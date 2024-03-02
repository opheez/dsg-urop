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

    public struct TupleId{ //} : IEquatable<TupleId>{

        public long Key;
        public Table Table;
        public TupleId(long key, Table t){
            Key = key;
            Table = t;
        }

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
        public KeyAttr(long key, long attr, Table t){
            Key = key;
            Attr = attr;
            Table = t;
        }
        public long Key;
        public long Attr;
        public Table Table;
        public static int Size = sizeof(long) * 2 + sizeof(int);

        public override string ToString(){
            return $"({Key}, {Attr})";
        }

        public byte[] ToBytes(){
            byte[] arr = new byte[Size];

            // Using MemoryMarshal to write the fixed-size fields to the byte array
            Span<byte> span = arr.AsSpan();
            MemoryMarshal.Write(span, ref Key);
            MemoryMarshal.Write(span.Slice(sizeof(long)), ref Attr);
            int tableHash = Table.GetHashCode();
            MemoryMarshal.Write(span.Slice(sizeof(long)*2), ref tableHash);

            return arr;
            // using (MemoryStream m = new MemoryStream()) {
            //     using (BinaryWriter writer = new BinaryWriter(m)) {
            //         writer.Write(Key);
            //         writer.Write(Attr);
            //         writer.Write(Table.GetHashCode());
            //     }
            //     return m.ToArray();
            // }
        }

        public static KeyAttr FromBytes(byte[] data, Dictionary<int, Table> tables) {
            KeyAttr result = new KeyAttr();

            Span<byte> span = data.AsSpan();
            result.Key = MemoryMarshal.Read<long>(span);
            result.Attr = MemoryMarshal.Read<long>(span.Slice(sizeof(long)));
            int tableHash = MemoryMarshal.Read<int>(span.Slice(sizeof(long)*2));
            result.Table = tables[tableHash];

            // result.Key = BitConverter.ToInt64(data, 0);
            // result.Attr = BitConverter.ToInt64(data, sizeof(long));
            // int tableHash = BitConverter.ToInt32(data, sizeof(long)*2);
            // result.Table = tables[tableHash];

            // using (MemoryStream m = new MemoryStream(data)) {
            //     using (BinaryReader reader = new BinaryReader(m)) {
            //         result.Key = reader.ReadInt64();
            //         result.Attr = reader.ReadInt64();
            //         int tableHash = reader.ReadInt32();
            //         result.Table = tables[tableHash];
            //     }
            // }
            return result;
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

        public static int GetLength(byte[][] arr){
            int len = 0;
            foreach (byte[] a in arr){
                len += a.Length;
            }
            return len;
        }
    }

}