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
/// Variable length pointers are stored as {size_of_variable}{address_of_variable}
/// Always uses ReadOnlySpan<byte> with the user
/// </summary>
public unsafe class Table : IDisposable{
    internal int rowSize;
    // TODO: bool can be a single bit
    internal Dictionary<long, (bool, int, int)> metadata; // (varLen, size, offset)
    internal ConcurrentDictionary<long, byte[]> data;
    // public Dictionary index; 

    public Table(Dictionary<long, (bool, int)> schema){
        this.metadata = new Dictionary<long,(bool, int, int)>();
        
        int offset = 0;
        int size = 0;
        foreach (var entry in schema) {
            if (!entry.Value.Item1 && entry.Value.Item2 <= 0) {
                throw new ArgumentException();
            }
            size = entry.Value.Item1 ? IntPtr.Size * 2 : entry.Value.Item2;
            this.metadata[entry.Key] = (entry.Value.Item1, size, offset);
            offset += entry.Value.Item1 ? IntPtr.Size * 2 : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<long, byte[]>();
    }

    internal ReadOnlySpan<byte> Read(long key, long attribute){
        if (!this.data.ContainsKey(key)){
            return ReadOnlySpan<byte>.Empty;
        }
        (bool varLen, int size, int offset) = this.metadata[attribute];
        if (varLen) {
            Pointer ptr = GetVarLenPtr(key, offset);
            return new ReadOnlySpan<byte>(ptr.Ptr, ptr.Size);
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, size);
    }

    public ReadOnlySpan<byte> Read(KeyAttr keyAttr, TransactionContext ctx){
        if (!this.metadata.ContainsKey(keyAttr.Attr)){
            throw new KeyNotFoundException();
        }

        byte[]? ctxRead = ctx.GetFromContext(keyAttr);
        if (ctxRead != null) {
            return ctxRead.AsSpan();
        } else {
            ReadOnlySpan<byte> currVal = this.Read(keyAttr.Key, keyAttr.Attr);
            return currVal;
        }
    }

    protected Pointer GetVarLenPtr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset + IntPtr.Size, IntPtr.Size)).ToArray();
        byte[] size = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        IntPtr res = new IntPtr(BitConverter.ToInt64(addr)); //TODO convert based on nint size
        return new Pointer(res, BitConverter.ToInt32(size));
    }
    internal void Upsert(long key, long attribute, ReadOnlySpan<byte> value){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
        byte[] valueToWrite = value.ToArray();
        if (varLen) {
            IntPtr addr = Marshal.AllocHGlobal(value.Length);
            Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
            valueToWrite = new byte[IntPtr.Size * 2];
            BitConverter.GetBytes(value.Length).CopyTo(valueToWrite, 0);
            BitConverter.GetBytes(addr.ToInt64()).CopyTo(valueToWrite, IntPtr.Size);
        }

        if (valueToWrite.Length != size || valueToWrite.Length <= 0) {
            throw new ArgumentException($"Value must be nonempty and equal to schema-specified size ({size})");
        }
        for (int i = 0; i < valueToWrite.Length; i++) {
            row[offset+i] = valueToWrite[i];
        }
    }

    public void Upsert(KeyAttr keyAttr, ReadOnlySpan<byte> value, TransactionContext ctx){
        if (!this.metadata.ContainsKey(keyAttr.Attr)){
            throw new KeyNotFoundException();
        }

        (bool varLen, int size, int offset) = this.metadata[keyAttr.Attr];
        if (!varLen && value.Length != size) {
            throw new ArgumentException($"Value to insert must be of size {size}");
        }
        ctx.SetInContext(keyAttr, value);
        return;
    }

    public void Dispose(){
        // iterate through all of the table to find pointers and dispose of 
        foreach (var field in metadata){
            if (field.Value.Item1 && field.Value.Item2 != -1) {
                int offset = field.Value.Item3;
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
            Console.Write($"{field.Key} is {field.Value.Item1} {field.Value.Item2} {field.Value.Item3}\n");
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

