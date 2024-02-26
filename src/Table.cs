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
    
    // will never return null, empty 
    virtual public ReadOnlySpan<byte> Read(TupleId tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);

        ReadOnlySpan<byte> value = ctx.GetFromReadset(tupleId);
        if (value == null) {
            value = Read(tupleId);
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(tupleId);
        if (changes.Item2 != null) {
            Span<byte> updatedValue = value.ToArray();
            foreach (TupleDesc td in changes.Item1) {
                int offset = this.metadata[td.Attr].Item2;
                changes.Item2.AsSpan(td.Offset, td.Size).CopyTo(updatedValue.Slice(offset));
            }
            result = updatedValue;
        } else {
            result = value;
        }
        // TODO: deal with varLen
        // project out the attributes
        ctx.AddReadSet(tupleId, result);
        return project(result, tupleDescs);
    }

    protected ReadOnlySpan<byte> project(ReadOnlySpan<byte> value, TupleDesc[] tupleDescs){
        // TODO: do this without allocating 
        int totalSize = tupleDescs[tupleDescs.Length - 1].Offset + tupleDescs[tupleDescs.Length - 1].Size;

        Span<byte> result = new byte[totalSize];
        foreach (TupleDesc td in tupleDescs){
            int offset = this.metadata[td.Attr].Item2;
            value.Slice(offset, td.Size).CopyTo(result.Slice(td.Offset, td.Size));
        }
        return result;
    }

    // Assumes attribute is valid 
    protected internal ReadOnlySpan<byte> Read(TupleId tupleId){
        if (!this.data.ContainsKey(tupleId.Key)){ // TODO: validate table
            return new byte[this.rowSize];
        }
        // TODO: deal with varLen 
        return this.data[tupleId.Key];
    }

    protected Pointer GetVarLenPtr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset + IntPtr.Size, IntPtr.Size)).ToArray();
        byte[] size = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        IntPtr res = new IntPtr(BitConverter.ToInt64(addr)); //TODO convert based on nint size
        return new Pointer(res, BitConverter.ToInt32(size));
    }

    /// <summary>
    /// Insert specified attributes into table. Non-specified attributes will be 0 
    /// </summary>
    /// <param name="tupleDescs"></param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <returns></returns>
    public TupleId Insert(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);

        long id = NewRecordId();
        TupleId tupleId = new TupleId(id, this);
        ctx.AddWriteSet(tupleId, tupleDescs, value);

        return tupleId;
    }

    /// <summary>
    /// Insert specified attributes into table. Non-specified attributes will be 0 
    /// </summary>
    /// <param name="tupleDescs"></param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <exception cref="ArgumentException">Key already exists</exception>
    /// <returns></returns>
    public void Insert(TupleId id, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        if (this.data.ContainsKey(id.Key)){
            throw new ArgumentException($"Key {id.Key} already exists in this table"); // TODO ensure this aborts transaction
        }
        Validate(tupleDescs, value, true);

        ctx.AddWriteSet(id, tupleDescs, value);

        return;
    }

    public void Update(TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);

        ctx.AddWriteSet(tupleId, tupleDescs, value);

    }

    /// <summary>
    /// Write value to specific attribute of key. If key does not exist yet, create empty row
    /// </summary>
    /// <param name="keyAttr"></param>
    /// <param name="value"></param>
    protected internal void Write(KeyAttr keyAttr, ReadOnlySpan<byte> value){
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

    public TupleDesc[] GetSchema(){
        TupleDesc[] schema = new TupleDesc[this.metadata.Count];
        for (int i = 0; i < this.metadata.Count; i++){
            long attr = this.metadataOrder[i];
            schema[i] = new TupleDesc(attr, this.metadata[attr].Item1, this.metadata[attr].Item2);
        }
        return schema;
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

    protected long NewRecordId() {
        return Interlocked.Increment(ref lastId);
    }

    protected void Validate(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, bool write) {
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

public class ShardedTable : Table {
    private RpcClient rpcClient;
    public ShardedTable((long, int)[] schema, RpcClient rpcClient) : base(schema) {
        this.rpcClient = rpcClient;
    }

    public override ReadOnlySpan<byte> Read(TupleId tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);

        ReadOnlySpan<byte> value = ctx.GetFromReadset(tupleId);
        if (value == null) {
            Console.WriteLine($"hashing to {rpcClient.HashKeyToDarqId(tupleId.Key)}");
            if (rpcClient.GetId() == rpcClient.HashKeyToDarqId(tupleId.Key)) {
                Console.WriteLine("actually reading own");
                value = Read(tupleId);
            } else {
                Console.WriteLine("actually reading rpc");
                value = rpcClient.Read(tupleId.Key, ctx);
            }
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(tupleId);
        if (changes.Item2 != null) {
            Span<byte> updatedValue = value.ToArray();
            foreach (TupleDesc td in changes.Item1) {
                int offset = this.metadata[td.Attr].Item2;
                changes.Item2.AsSpan(td.Offset, td.Size).CopyTo(updatedValue.Slice(offset));
            }
            result = updatedValue;
        } else {
            result = value;
        }
        // TODO: deal with varLen
        // project out the attributes
        ctx.AddReadSet(tupleId, result);
        return project(result, tupleDescs);
    }
}
}