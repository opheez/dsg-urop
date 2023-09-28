using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Runtime.CompilerServices;

[assembly:InternalsVisibleTo("TableTests")]
namespace DB { 

/// <summary>
/// Stores data as a byte array (TODO: change to generic type)
/// Variable length pointers are stored as {size_of_variable}{address_of_variable} and have a size of IntPtr.Size * 2
/// Always uses ReadOnlySpan<byte> with the user
/// Assumes schema is never changed after creation 
/// </summary>
public unsafe class Table : IDisposable{
    private long lastId = 0;
    internal int rowSize;
    // TODO: bool can be a single bit
    internal long[] metadataOrder;
    internal Dictionary<long, (int, int)> metadata; // (size, offset), size=-1 if varLen
    internal ConcurrentDictionary<long, byte[]> data;
    // public Dictionary index; 

    public Table((long, int)[] schema){
        this.metadata = new Dictionary<long,(int, int)>();
        this.metadataOrder = new long[schema.Length];
        
        int offset = 0;
        int size = 0;
        for(int i = 0; i < schema.Length; i++) {
            long attr = schema[i].Item1;
            size = schema[i].Item2;
            if (size <= 0 && size != -1) {
                throw new ArgumentException();
            }
            this.metadata[attr] = (size, offset);
            this.metadataOrder[i] = attr;
            offset += (size == -1) ? IntPtr.Size * 2 : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<long, byte[]>();
    }

    public ReadOnlySpan<byte> Read(TupleId tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);
        var ctxRead = ctx.GetFromContext(tupleId);
        if (ctxRead != null) {
            return ctxRead.AsSpan();
        }
        return this.Read(tupleId.Key, tupleDescs);
    }

    internal ReadOnlySpan<byte> Read(long key, TupleDesc[] tupleDescs){
        if (!this.data.ContainsKey(key)){
            return ReadOnlySpan<byte>.Empty;
        }
        List<byte> result = new();
        byte[] tuple = this.data[key];
        int writeOffset = 0;
        foreach (TupleDesc desc in tupleDescs) {
            (int size, int readOffset) = this.metadata[desc.Attr];
            if (size == -1) {
                Pointer ptr = GetVarLenPtr(key, readOffset);
                result.AddRange(new ReadOnlySpan<byte>(ptr.Ptr, ptr.Size).ToArray());
                size = ptr.Size;
            } else {
                for (int i = readOffset; i < readOffset+size; i++) {
                    result.Add(tuple[i]);
                }
            }
            writeOffset += size;
        }
        return result.ToArray();
    }

    protected Pointer GetVarLenPtr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset + IntPtr.Size, IntPtr.Size)).ToArray();
        byte[] size = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        IntPtr res = new IntPtr(BitConverter.ToInt64(addr)); //TODO convert based on nint size
        return new Pointer(res, BitConverter.ToInt32(size));
    }
    
    // internal void Upsert(long key, ReadOnlySpan<byte> value){
    //     foreach (KeyValuePair<long, (int, int)> entry in this.metadata) {
    //         (int size, int offset) = entry.Value;
    //         Upsert(key, entry.Key, value.Slice(offset, size));
    //     }
    // }

    // internal void Upsert(long key, long attribute, ReadOnlySpan<byte> value){
    //     (int size, int offset) = this.metadata[attribute];
    //     byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
    //     byte[] valueToWrite = value.ToArray();
    //     if (size == -1) {
    //         IntPtr addr = Marshal.AllocHGlobal(value.Length);
    //         Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
    //         valueToWrite = new byte[IntPtr.Size * 2];
    //         BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
    //         BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
    //     }

    //     if (valueToWrite.Length <= 0 || (size != -1 && valueToWrite.Length != size)) {
    //         throw new ArgumentException($"Value must be nonempty and equal to schema-specified size ({size})");
    //     }
    //     for (int i = 0; i < valueToWrite.Length; i++) {
    //         row[offset+i] = valueToWrite[i];
    //     }
    // }

    // /// <summary>
    // /// Transactionally updates
    // /// </summary>
    // /// <param name="tupleId"></param>
    // /// <param name="value"></param>
    // /// <param name="ctx"></param>
    // /// <exception cref="KeyNotFoundException"></exception>
    // /// <exception cref="ArgumentException"></exception>
    // public void Upsert(TupleId tupleId, ReadOnlySpan<byte> value, TransactionContext ctx){
    //     if (tupleId.Attr.HasValue) {
    //         if (!this.metadata.ContainsKey(tupleId.Attr.Value)){
    //             throw new KeyNotFoundException();
    //         }

    //         (int size, int offset) = this.metadata[tupleId.Attr.Value];
    //         if (size != -1 && value.Length != size) {
    //             throw new ArgumentException($"Value to insert must be of size {size}");
    //         }
    //     } // TODO: add assertion checks if possible?
    //     ctx.SetInContext(tupleId, value);
    //     return;
    // }

    public TupleId Insert(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);

        long id = NewRecordId();
        TupleId tupleId = new(id, this);

        ctx.SetInContext(OperationType.Insert, tupleId, tupleDescs, value);
        return new TupleId(id, this);
    }

    internal void Insert(long key, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value){
        if (this.data.ContainsKey(key)) {
            throw new ArgumentException($"Key {key} already exists");
        }
        this.data[key] = new byte[rowSize];
        Write(key, tupleDescs, value);
    }

    public void Update(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);

        ctx.SetInContext(OperationType.Update, tupleId, tupleDescs, value);
    }

    internal void Update(long key, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value){
        if (!this.data.ContainsKey(key)) {
            throw new ArgumentException($"Key {key} does not exist: try inserting instead");
        }
        Write(key, tupleDescs, value);
    }

    // tupleDescs: varlenattr, size = 
    internal void Write(long key, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value){
        int readOffset = 0;
        foreach (TupleDesc desc in tupleDescs) {
            (int size, int writeOffset) = this.metadata[desc.Attr];
            byte[] valueToWrite = value.Slice(readOffset, desc.Size).ToArray(); //TODO: possibly optimize and not ToArray()
            if (size == -1) {
                IntPtr addr = Marshal.AllocHGlobal(desc.Size);
                Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
                valueToWrite = new byte[IntPtr.Size * 2];
                BitConverter.GetBytes(desc.Size).CopyTo(valueToWrite, 0);
                BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
            }
            for (int i = 0; i < valueToWrite.Length; i++) {
                this.data[key][writeOffset+i] = valueToWrite[i];
            }
            readOffset += desc.Size;
        }
    }

    public void Dispose(){
        // iterate through all of the table to find pointers and dispose of 
        foreach (var field in metadata){
            if (field.Value.Item1 == -1) {
                int offset = field.Value.Item2;
                foreach (var entry in data){
                    IntPtr ptrToFree = GetVarLenPtr(entry.Key, offset).IntPointer;
                    if (ptrToFree != IntPtr.Zero){
                        Marshal.FreeHGlobal(ptrToFree);
                    }
                }
            }
        }
    }

    public void Debug(){
        Console.WriteLine("Metadata: ");
        foreach (var field in metadata){
            Console.Write($"{field.Key} is {field.Value.Item1 == -1} {field.Value.Item1} {field.Value.Item2}\n");
            // for (int i=0; i < entry.Value.Length; i++) {
            //     System.Console.Write(entry.Value[i] + ",");
            // }
            // Console.WriteLine("\n");// + Encoding.ASCII.GetBytes(entry.Value));
        }
        foreach (var entry in data){
            Console.WriteLine(entry.Key);
            for (int i=0; i < entry.Value.Length; i++) {
                System.Console.Write(entry.Value[i] + ",");
            }
            Console.WriteLine("\n");// + Encoding.ASCII.GetBytes(entry.Value));
        }
    }

    private long NewRecordId() {
        return Interlocked.Increment(ref lastId);
    }

    private void Validate(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, bool write) {
        int totalSize = 0;
        foreach (TupleDesc desc in tupleDescs) {
            if (!this.metadata.ContainsKey(desc.Attr)) {
                throw new ArgumentException($"Attribute {desc.Attr} is not a valid attribute for this table");
            }
            totalSize += desc.Size;
        }
        Console.WriteLine($"{write} {totalSize} = {value.Length}?");
        if (write && totalSize != value.Length) {
            throw new ArgumentException($"Expected size {totalSize} from tuple description but instead got size {value.Length}");
        }
    }

}

}

