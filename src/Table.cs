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
    // delegate void LogWAL();
    internal LogWAL logWAL;


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

    public Table((long, int)[] schema, LogWAL log) : this(schema) {
        this.logWAL = log;
    }

    public ReadOnlySpan<byte> Read(TupleId tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false); //TODO: behavior if it doesnt contain key?

        if (tupleDescs.Length == 1) {
            return Read(new KeyAttr(tupleId.Key, tupleDescs[0].Attr, this));
        }

        List<byte> result = new();
        int writeOffset = 0; // TODO: need to return tupleDesc for varLen
        foreach (TupleDesc desc in tupleDescs) {
            byte[] valueToWrite;
            (int size, int readOffset) = this.metadata[desc.Attr];
            KeyAttr ka = new KeyAttr(tupleId.Key, desc.Attr, this);
            var ctxRead = ctx.GetFromContext(ka);
            if (ctxRead != null) {
                valueToWrite = ctxRead;
            } else {
                valueToWrite = this.Read(new KeyAttr(tupleId.Key, desc.Attr, this)).ToArray();
                ctx.SetInContext(OperationType.Read, ka, valueToWrite);
            }
            result.AddRange(valueToWrite);
            writeOffset += valueToWrite.Length;
        }

        return result.ToArray();
    }

    // Assumes attribute is valid 
    internal ReadOnlySpan<byte> Read(KeyAttr keyAttr){
        if (!this.data.ContainsKey(keyAttr.Key)){ // TODO: validate table
            return ReadOnlySpan<byte>.Empty;
        }
        (int size, int readOffset) = this.metadata[keyAttr.Attr];
        if (size == -1) {
            Pointer ptr = GetVarLenPtr(keyAttr.Key, readOffset);
            return new ReadOnlySpan<byte>(ptr.Ptr, ptr.Size).ToArray();
        } 
        return this.data[keyAttr.Key].AsSpan().Slice(readOffset, size);
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
        TupleId tupleId = new TupleId(id, this.GetHashCode());
        foreach (TupleDesc desc in tupleDescs) {
            ctx.SetInContext(OperationType.Insert, new KeyAttr(id, desc.Attr ,this), value);
        }

        return tupleId;
    }
    public void Insert(TupleId id, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        if (this.data.ContainsKey(id.Key)){
            throw new ArgumentException($"Key {id.Key} already exists in this table"); // TODO ensure this aborts transaction
        }
        Validate(tupleDescs, value, true);

        foreach (TupleDesc desc in tupleDescs) {
            ctx.SetInContext(OperationType.Insert, new KeyAttr(id.Key, desc.Attr ,this), value);
        }

        return;
    }

    // internal void Insert(KeyAttr keyAttr, ReadOnlySpan<byte> value){
    //     if (!Util.IsEmpty(Read(keyAttr))) { // should not happen if called by transaction context
    //         throw new ArgumentException($"!!! This should not be thrown. Key and attribute ({keyAttr}) already exists");
    //     }
    //     Write(keyAttr, value);
    // }

    public void Update(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);
        foreach (TupleDesc desc in tupleDescs) {
            // if (Util.IsEmpty(Read(tupleId, new TupleDesc[]{desc}, ctx))){ 
            //     throw new ArgumentException($"Key ({tupleId.Key}, {desc.Attr}) does not exist: try inserting instead"); // TODO ensure this aborts transaction
            // }
            ctx.SetInContext(OperationType.Update, new KeyAttr(tupleId.Key, desc.Attr, this), value);
        }
    }

    // internal void Update(KeyAttr keyAttr, ReadOnlySpan<byte> value){
    //     // if (Util.IsEmpty(Read(keyAttr))) { // should not happen if called by transaction context
    //     //     throw new ArgumentException($"!!! This should not be called. Key {keyAttr} does not exist: try inserting instead");
    //     // }
    //     Write(keyAttr, value);
    // }

    internal void Write(KeyAttr keyAttr, ReadOnlySpan<byte> value){
        this.data.TryAdd(keyAttr.Key, new byte[rowSize]);
        (int size, int offset) = this.metadata[keyAttr.Attr];
        byte[] valueToWrite = value.ToArray(); //TODO: possibly optimize and not ToArray()
        if (size == -1) {
            IntPtr addr = Marshal.AllocHGlobal(value.Length);
            Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
            valueToWrite = new byte[IntPtr.Size * 2];
            BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
            BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
        }
        for (int i = 0; i < valueToWrite.Length; i++) {
            this.data[keyAttr.Key][offset+i] = valueToWrite[i];
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
            if (this.metadata[desc.Attr].Item1 != -1 && desc.Size != this.metadata[desc.Attr].Item1) {
                throw new ArgumentException($"Expected size {this.metadata[desc.Attr].Item1} for attribute {desc.Attr} but instead got size {desc.Size}");
            }
            totalSize += desc.Size;
        }
        if (write && totalSize != value.Length) {
            throw new ArgumentException($"Expected size {totalSize} from tuple description but instead got size {value.Length}");
        }
    }

}

}

