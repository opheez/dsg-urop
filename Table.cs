using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

// namespace TableNS{ 
public unsafe class Table{
    internal uint id;
    internal long size;
    internal int rowSize;
    // TODO: bool can be a single bit
    internal ConcurrentDictionary<long, (bool, int, int)> metadata;
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

    public ReadOnlySpan<byte> Get(long key, long attribute){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        if (varLen) {
            byte* ptr = GetVarLenAddr(key, offset);
            return new ReadOnlySpan<byte>(ptr, size);
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, size);
    }

    internal byte* GetVarLenAddr(long key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        // System.Console.WriteLine(BitConverter.ToInt64(addr));
        byte* ptr = (byte*)(new IntPtr(BitConverter.ToInt64(addr))).ToPointer();
        return ptr;
    }
    public ReadOnlySpan<byte> Set(long key, long attribute, byte[] value){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
        byte[] valueToWrite = value;
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

    // public void debug(){
    //     foreach (var entry in data){
    //         Console.WriteLine(entry.Key);
    //         for (int i=0; i < entry.Value.Length; i++) {
    //             System.Console.Write(entry.Value[i] + ",");
    //         }
    //         Console.WriteLine("\n" + Encoding.ASCII.Getlong(entry.Value));
    //     }
    // }

}

// }

