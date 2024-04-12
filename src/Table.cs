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
    private int id;
    private long lastId = 0;
    public int rowSize;
    // TODO: bool can be a single bit
    internal long[] metadataOrder;
    internal Dictionary<long, (int, int)> metadata; // (size, offset), size=-1 if varLen
    internal ConcurrentDictionary<PrimaryKey, byte[]> data;

    // Secondary index
    protected ConcurrentDictionary<byte[], PrimaryKey> secondaryIndex;

    protected ILogger logger;

    public Table(int id, (long, int)[] schema, ILogger logger = null){
        this.id = id;
        this.metadata = new Dictionary<long,(int, int)>();
        this.metadataOrder = new long[schema.Length];
        this.logger = logger;
        
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
        this.data = new ConcurrentDictionary<PrimaryKey, byte[]>();
    }
    
    // will never return null, empty 
    virtual public ReadOnlySpan<byte> Read(PrimaryKey tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);
        PrintDebug($"Reading normal {tupleId}", ctx);

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

    virtual public (byte[], PrimaryKey) ReadSecondary(byte[] key, TupleDesc[] tupleDescs, TransactionContext ctx){
        if (secondaryIndex == null){
            throw new InvalidOperationException("Secondary index not set");
        }
        PrimaryKey pk = secondaryIndex[key];
        return (Read(pk, tupleDescs, ctx).ToArray(), pk);
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
    protected internal ReadOnlySpan<byte> Read(PrimaryKey tupleId){
        if (!this.data.ContainsKey(tupleId)){ // TODO: validate table
            return new byte[this.rowSize];
        }
        // TODO: deal with varLen 
        return this.data[tupleId];
    }

    protected Pointer GetVarLenPtr(PrimaryKey key, int offset){
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
    public PrimaryKey Insert(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        Validate(tupleDescs, value, true);

        long id = NewRecordId(); // TODO: make sure this new record id falls within range of this partition in shardedBenchmark
        PrimaryKey tupleId = new PrimaryKey(this.id, id);
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
    /// <returns>whether insert succeeded</returns>
    public bool Insert(PrimaryKey id, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        PrintDebug($"Inserting {id}", ctx);
        if (this.data.ContainsKey(id)){
            return false;
        }
        Validate(tupleDescs, value, true);

        ctx.AddWriteSet(id, tupleDescs, value);

        return true;
    }

    public void Update(PrimaryKey tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
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

    public void SetSecondaryIndex(ConcurrentDictionary<byte[], PrimaryKey> index){
        secondaryIndex = index;
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

    public (int, int) GetAttrMetadata(long attr){
        return this.metadata[attr];
    }

    public int GetId(){
        return this.id;
    }

    virtual public void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[Table TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }

    public void PrintTable(){
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
    // extracts relevant values from secondary key to primary key for correct shard
    protected Func<byte[], PrimaryKey> buildTempPk;
    public ShardedTable(int id, (long, int)[] schema, RpcClient rpcClient, ILogger logger = null) : base(id, schema, logger) {
        this.rpcClient = rpcClient;
    }

    public override ReadOnlySpan<byte> Read(PrimaryKey tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);
        PrintDebug($"Reading {tupleId}", ctx);

        ReadOnlySpan<byte> value = ctx.GetFromReadset(tupleId);
        if (value == null) {
            if (rpcClient.IsLocalKey(tupleId)) {
                value = Read(tupleId);
            } else {
                value = rpcClient.Read(tupleId, ctx);
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

    override public (byte[], PrimaryKey) ReadSecondary(byte[] key, TupleDesc[] tupleDescs, TransactionContext ctx){
        if (secondaryIndex == null){
            throw new InvalidOperationException("Secondary index not set");
        }
        ReadOnlySpan<byte> value;
        PrimaryKey pk;
        PrimaryKey tempPk = buildTempPk(key);
        if (rpcClient.IsLocalKey(tempPk)){
            pk = secondaryIndex[key];
            value = Read(pk, tupleDescs, ctx).ToArray();
        } else {
            (value, pk) = rpcClient.ReadSecondary(tempPk, key, ctx);
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(pk);
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
        ctx.AddReadSet(pk, result);
        return (project(result, tupleDescs).ToArray(), pk);
    }

    public void SetSecondaryIndex(ConcurrentDictionary<byte[], PrimaryKey> index, Func<byte[], PrimaryKey> buildTempPk){
        this.buildTempPk = buildTempPk;
        foreach (var entry in index){
            PrimaryKey tempPk = buildTempPk(entry.Key);
            if (rpcClient.IsLocalKey(tempPk)){
                secondaryIndex = index;
            } else {
                rpcClient.SetSecondaryIndex(tempPk, index);
            }
            break;
        }
    }

    override public void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[ST {rpcClient.GetId()} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
}
}