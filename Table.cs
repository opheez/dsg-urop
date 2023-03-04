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
    internal ConcurrentDictionary<string, (bool, int, int)> metadata;
    internal ConcurrentDictionary<string, byte[]> data;
    // public Dictionary index; 

    public Table(Dictionary<string, (bool, int)> schema){
        this.metadata = new ConcurrentDictionary<string,(bool, int, int)>();
        
        int offset = 0;
        int size = 0;
        foreach (var entry in schema) {
            size = entry.Value.Item1 ? -1 : entry.Value.Item2;
            this.metadata[entry.Key] = (entry.Value.Item1, size, offset);
            offset += entry.Value.Item1 ? IntPtr.Size : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<string, byte[]>();
    }

    public ReadOnlySpan<byte> Get(string key, string attribute){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        if (varLen) {
            byte* ptr = GetVarLenAddr(key, offset);
            return new ReadOnlySpan<byte>(ptr, size);
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, offset+size);
    }

    internal byte* GetVarLenAddr(string key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset, offset+IntPtr.Size)).ToArray();
        // System.Console.WriteLine(BitConverter.ToInt64(addr));
        byte* ptr = (byte*)(new IntPtr(BitConverter.ToInt64(addr))).ToPointer();
        return ptr;
    }
    public ReadOnlySpan<byte> Set(string key, string attribute, byte[] value){
        (bool varLen, int size, int offset) = this.metadata[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]); //TODO: check if written before to free pointer
        byte[] valueToWrite = value;
        if (varLen) {
            this.metadata[attribute] = (varLen, value.Length, offset);
            IntPtr addr = Marshal.AllocHGlobal(value.Length);
            Marshal.Copy(valueToWrite, 0, addr, valueToWrite.Length);
            valueToWrite = BitConverter.GetBytes(addr.ToInt64()); // TODO: change based on size of intptr
            size = IntPtr.Size;
            // fixed (byte* valuePtr = &cloned[0]) {
                // IntPtr addr = new IntPtr(valuePtr);
                // System.Console.WriteLine(addr.ToInt64());
                // valueToWrite = BitConverter.GetBytes(addr.ToInt64()); // TODO: change based on size of intptr
            // }
        }
        // TODO: Alternatively, look into Marshal?
        for (int i = 0; i < valueToWrite.Length; i++) {
            if (offset + i > size) break;
            row[offset+i] = valueToWrite[i];
        }
        return new ReadOnlySpan<byte>(this.data[key], offset, offset+size);
    }

    // public void debug(){
    //     foreach (var entry in data){
    //         Console.WriteLine(entry.Key);
    //         for (int i=0; i < entry.Value.Length; i++) {
    //             System.Console.Write(entry.Value[i] + ",");
    //         }
    //         Console.WriteLine("\n" + Encoding.ASCII.GetString(entry.Value));
    //     }
    // }

}

// }

