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
/// </summary>
public unsafe class Table : IDisposable{
    internal int rowSize;
    // TODO: bool can be a single bit
    internal Dictionary<long, (int, int)> metadata; // (size, offset), size=-1 if varLen
    internal ConcurrentDictionary<long, byte[]> data;
    // public Dictionary index; 

    public Table(Dictionary<long, (bool, int)> schema){
        this.metadata = new Dictionary<long,(int, int)>();
        
        int offset = 0;
        int size = 0;
        foreach (var entry in schema) {
            if (!entry.Value.Item1 && entry.Value.Item2 <= 0) {
                throw new ArgumentException();
            }
            size = entry.Value.Item1 ? -1 : entry.Value.Item2;
            this.metadata[entry.Key] = (size, offset);
            offset += entry.Value.Item1 ? IntPtr.Size * 2 : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<long, byte[]>();
    }

    internal ReadOnlySpan<byte> Read(long key, long attribute){
        if (!this.data.ContainsKey(key)){
            return ReadOnlySpan<byte>.Empty;
        }
        (int size, int offset) = this.metadata[attribute];
        if (size == -1) {
            Pointer ptr = GetVarLenPtr(key, offset);
            return new ReadOnlySpan<byte>(ptr.Ptr, ptr.Size);
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, size);
    }

    /// <summary>
    /// Reads entire record by repeatedly reading its attributes 
    /// and stitching it together in a single span
    /// </summary>
    /// <param name="key">key to read</param>
    /// <returns></returns> <summary>
    internal ReadOnlySpan<byte> Read(long key){
        if (!this.data.ContainsKey(key)){
            return ReadOnlySpan<byte>.Empty;
        }
        // Span<byte> result = new byte[rowSize];
        List<byte> result = new List<byte>();
        foreach (KeyValuePair<long, (int, int)> entry in this.metadata) {
            (int size, int offset) = entry.Value;
            // Read(key, entry.Key).CopyTo(result.Slice(offset, size));
            result.AddRange(Read(key, entry.Key).ToArray());
        }
        return result.ToArray();
    }

    public ReadOnlySpan<byte> Read(KeyAttr keyAttr, TransactionContext ctx){
        if (keyAttr.Attr.HasValue) {
            long attr = keyAttr.Attr.Value;
            if (!this.metadata.ContainsKey(attr)){
                throw new KeyNotFoundException();
            }
        
            byte[]? ctxRead = ctx.GetFromContext(keyAttr);
            if (ctxRead != null) {
                return ctxRead.AsSpan();
            } else {
                ReadOnlySpan<byte> currVal = this.Read(keyAttr.Key, attr);
                return currVal;
            }
        } else { // Read entire record
            byte[]? ctxRead = ctx.GetFromContext(keyAttr);
            if (ctxRead != null) {
                return ctxRead.AsSpan();
            } else {
                ReadOnlySpan<byte> currVal = this.Read(keyAttr.Key);
                return currVal;
            }
        }
    }

    protected Pointer GetVarLenPtr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset + IntPtr.Size, IntPtr.Size)).ToArray();
        byte[] size = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        IntPtr res = new IntPtr(BitConverter.ToInt64(addr)); //TODO convert based on nint size
        return new Pointer(res, BitConverter.ToInt32(size));
    }
    
    internal void Upsert(long key, ReadOnlySpan<byte> value){
        foreach (KeyValuePair<long, (int, int)> entry in this.metadata) {
            (int size, int offset) = entry.Value;
            Upsert(key, entry.Key, value.Slice(offset, size));
        }
    }

    internal void Upsert(long key, long attribute, ReadOnlySpan<byte> value){
        (int size, int offset) = this.metadata[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
        byte[] valueToWrite = value.ToArray();
        if (size == -1) {
            IntPtr addr = Marshal.AllocHGlobal(value.Length);
            Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
            valueToWrite = new byte[IntPtr.Size * 2];
            BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
            BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
        }

        if (valueToWrite.Length <= 0 || (size != -1 && valueToWrite.Length != size)) {
            throw new ArgumentException($"Value must be nonempty and equal to schema-specified size ({size})");
        }
        for (int i = 0; i < valueToWrite.Length; i++) {
            row[offset+i] = valueToWrite[i];
        }
    }

    /// <summary>
    /// Transactionally updates
    /// </summary>
    /// <param name="keyAttr"></param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <exception cref="KeyNotFoundException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public void Upsert(KeyAttr keyAttr, ReadOnlySpan<byte> value, TransactionContext ctx){
        if (keyAttr.Attr.HasValue) {
            if (!this.metadata.ContainsKey(keyAttr.Attr.Value)){
                throw new KeyNotFoundException();
            }

            (int size, int offset) = this.metadata[keyAttr.Attr.Value];
            if (size != -1 && value.Length != size) {
                throw new ArgumentException($"Value to insert must be of size {size}");
            }
        } // TODO: add assertion checks if possible?
        ctx.SetInContext(keyAttr, value);
        return;
    }

    // /// <summary>
    // /// Insert record
    // /// </summary>
    // /// <param name="key"></param>
    // /// <param name="value"></param>
    // /// <exception cref="ArgumentException">Throws if key already exists</exception>
    // internal void Insert(long key, ReadOnlySpan<byte> value){
    //     if (this.data.ContainsKey(key)){
    //         throw new ArgumentException($"Key {key} already exists");
    //     }
    //     byte[] row = new byte[this.rowSize];
    //     byte[] valueToWrite = value.ToArray();
    //     foreach (KeyValuePair<long, (int,int)> entry in this.metadata) {
    //         (int size, int offset) = entry.Value;
    //         if (size == -1) {
    //             IntPtr addr = Marshal.AllocHGlobal(value.Length);
    //             Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
    //             valueToWrite = new byte[IntPtr.Size * 2];
    //             BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
    //             BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
    //         }
    //         for (int i = 0; i < valueToWrite.Length; i++) {
    //             row[offset+i] = valueToWrite[i];
    //         }
    //     }
    //     this.data[key] = row;
    // }


    // /// <summary>
    // /// Updates record by attribute
    // /// </summary>
    // /// <param name="key"></param>
    // /// <param name="attribute"></param>
    // /// <param name="value"></param>
    // /// <exception cref="KeyNotFoundException">Throws if key is not in data or if attribute is not in metadata </exception>
    // /// <exception cref="ArgumentException">Throws if value is empty or not equal to specified size for non-variable length attributes</exception>
    // internal void Update(long key, long attribute, ReadOnlySpan<byte> value){
    //     (int size, int offset) = this.metadata[attribute];
    //     byte[] row = this.data[key];
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
    // /// Updates entire record 
    // /// </summary>
    // /// <param name="key"></param>
    // /// <param name="value"></param>
    // /// <exception cref="KeyNotFoundException">Throws if key is not in data or if attribute is not in metadata </exception>
    // /// <exception cref="ArgumentException">Throws if value is empty or not equal to the table's rowSize</exception>
    // internal void Update(long key, ReadOnlySpan<byte> value){
    //     byte[] row = this.data[key];
    //     if (value.Length == 0 || value.Length != this.rowSize) {
    //         throw new ArgumentException($"Value must be span of length {this.rowSize}");
    //     }
    //     foreach (KeyValuePair<long, (int,int)> entry in this.metadata) {
    //         (int size, int offset) = entry.Value;
    //         byte[] valueToWrite = value.Slice(offset, size).ToArray();
    //         if (size == -1) {
    //             IntPtr addr = Marshal.AllocHGlobal(value.Length);
    //             Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
    //             valueToWrite = new byte[IntPtr.Size * 2];
    //             BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
    //             BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
    //         }
    //         for (int i = 0; i < valueToWrite.Length; i++) {
    //             row[offset+i] = valueToWrite[offset+i];
    //         }
    //     }
    // }

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

    public void debug(){
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

}

}

