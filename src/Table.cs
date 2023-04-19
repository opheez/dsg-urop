using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Runtime.CompilerServices;

[assembly:InternalsVisibleTo("TableTests")]
namespace DB { 

/// <summary>
/// Stores data as a byte array (TODO: change to generic type)
/// Always uses Span<byte> with the user
/// </summary>
public unsafe class Table : IDisposable{
    internal uint id;
    internal long size;
    internal int rowSize;
    // TODO: bool can be a single bit
    internal ConcurrentDictionary<long, (bool, int, int)> metadata; // (varLen, size, offset)
    internal ConcurrentDictionary<long, byte[]> data;
    // public Dictionary index; 

    public Table(Dictionary<long, (bool, int)> schema){
        this.metadata = new ConcurrentDictionary<long,(bool, int, int)>();
        
        int offset = 0;
        int size = 0;
        foreach (var entry in schema) {
            if (!entry.Value.Item1 && entry.Value.Item2 <= 0) {
                throw new ArgumentException();
            }
            size = entry.Value.Item1 ? -1 : entry.Value.Item2;
            this.metadata[entry.Key] = (entry.Value.Item1, size, offset);
            offset += entry.Value.Item1 ? IntPtr.Size : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<long, byte[]>();
    }

    internal ReadOnlySpan<byte> Read(long key, long attribute){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        if (varLen) {
            byte* ptr = GetVarLenPtr(key, offset);
            return new ReadOnlySpan<byte>(ptr, size);
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, size);
    }

    // public ReadOnlySpan<byte> Read(KeyAttr keyAttr, TransactionContext ctx){
    //     // TODO: raise wrong table error if KeyAttr does not exist in this table

    //     // if it does, read from the table and add it to the context
    // }

    protected byte* GetVarLenPtr(long key, int offset){
        return (byte*)(GetVarLenAddr(key, offset)).ToPointer();
    }
    protected IntPtr GetVarLenAddr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        // Console.WriteLine(addr.ToString());
        return new IntPtr(BitConverter.ToInt64(addr));
    }
    internal ReadOnlySpan<byte> Upsert(long key, long attribute, Span<byte> value){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
        byte[] valueToWrite = value.ToArray();
        if (varLen) {
            this.metadata[attribute] = (varLen, value.Length, offset);
            IntPtr addr = Marshal.AllocHGlobal(value.Length);
            Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
            valueToWrite = BitConverter.GetBytes(addr.ToInt64()); // TODO: change based on size of intptr
            size = IntPtr.Size;
        } else if (value.Length > size || value.Length <= 0) {
            throw new ArgumentException("Value must be nonempty and less than schema-specified size");
        }
        for (int i = 0; i < valueToWrite.Length; i++) {
            row[offset+i] = valueToWrite[i];
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, size);
    }

    // public ReadOnlySpan<byte> Upsert(KeyAttr keyAttr, Span<byte> value, TransactionContext ctx){
    //     ctx.Set(keyAttr, value);
    // }

    public void Dispose(){
        // iterate through all of the table to find pointers and dispose of 
        foreach (var field in metadata){
            if (field.Value.Item1 && field.Value.Item2 != -1) {
                int offset = field.Value.Item3;
                foreach (var entry in data){
                    IntPtr ptrToFree = GetVarLenAddr(entry.Key, offset);
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

