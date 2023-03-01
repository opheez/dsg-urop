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
    // public FieldInfo[] fields;
    public ConcurrentDictionary<string, (int, int)> catalog;
    public ConcurrentDictionary<string, byte[]> data; // {ch[]: byte[]} | ch[]: tuple
    // public Dictionary index; 

    // TODO: using dynamic vs T?? in the future, could be interface! 
    public Table(Dictionary<string, int> schema){
        this.catalog = new ConcurrentDictionary<string,(int, int)>();
        
        int offset = 0;
        int size = 0;
        foreach (var entry in schema) {
            size = entry.Value;
            this.catalog[entry.Key] = (size, offset);
            offset += size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<string, byte[]>();
    }

    public ReadOnlySpan<byte> Get(string key, string attribute){
        (int size, int offset) = this.catalog[attribute];
        return new ReadOnlySpan<byte>(this.data[key], offset, offset+size);
    }
    public void Set(string key, string attribute, byte[] value){
        (int size, int offset) = this.catalog[attribute];
        byte[] row = this.data.GetOrAdd(key, new byte[this.rowSize]);
        for (int i = 0; i < value.Length; i++) {
            if (offset + i > size) break;
            row[offset+i] = value[i];
        }
        // Span<byte> slot = new Span<byte>(row, offset, offset+size);
        // slot = value;
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

