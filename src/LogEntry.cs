
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.InteropServices;

namespace DB {

public enum LogType {
    Begin,
    Prepare,
    Commit,
    Abort,
    Write
}

public struct LogEntry{
    public bool persited = false;
    public long lsn;
    public long prevLsn; // do we even need this if we are undoing?
    public long tid;
    public LogType type;
    public KeyAttr keyAttr;
    public byte[] val;
    public static readonly int Size = sizeof(long) * 3 + sizeof(int) + KeyAttr.Size;
    public LogEntry(long prevLsn, long tid, KeyAttr keyAttr, byte[] val){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.keyAttr = keyAttr;
        this.val = val;
        this.type = LogType.Write;
    }

    public LogEntry(long prevLsn, long tid, LogType type){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = type;
    }

    public void SetPersisted(){
        persited = true;
    }

    public byte[] ToBytes(){
        int totalSize = Size + (val != null ? val.Length : 0);

        byte[] arr = new byte[totalSize];

        // Using MemoryMarshal to write the fixed-size fields to the byte array
        Span<byte> span = arr.AsSpan();
        MemoryMarshal.Write(span, ref lsn);
        MemoryMarshal.Write(span.Slice(sizeof(long)), ref prevLsn);
        MemoryMarshal.Write(span.Slice(sizeof(long)*2), ref tid);
        int typeAsInt = (int)type;
        MemoryMarshal.Write(span.Slice(sizeof(long)*3), ref typeAsInt);
        
        // Write the variable-sized byte array to the byte array
        if (type == LogType.Write){
            byte[] keyAttrBytes = keyAttr.ToBytes();
            keyAttrBytes.CopyTo(span.Slice(sizeof(long)*3+sizeof(int)));
            val.CopyTo(span.Slice(Size));
        }

        return arr;
        // using (MemoryStream m = new MemoryStream()) {
        //     BinaryFormatter bf = new BinaryFormatter();
        //     using (BinaryWriter writer = new BinaryWriter(m)) {
        //         writer.Write(lsn);
        //         writer.Write(prevLsn);
        //         writer.Write(tid);
        //         writer.Write((int)type);
        //         if (type == LogType.Write){
        //             writer.Write(keyAttr.ToBytes());
        //             writer.Write(val.Length);
        //             writer.Write(val);
        //         }
        //     }
        //     return m.ToArray();
        // }
    }

    public static LogEntry FromBytes(byte[] data, Dictionary<int, Table> tables) {
        // Ensure that the data array has enough bytes for the struct
        if (data.Length < Size) throw new ArgumentException("Insufficient data to deserialize the struct.");

        LogEntry result = new LogEntry();

        // Using MemoryMarshal to read the fixed-size fields from the byte array
        Span<byte> span = data.AsSpan();
        result.lsn = MemoryMarshal.Read<long>(span.Slice(0, sizeof(long)));
        result.prevLsn = MemoryMarshal.Read<long>(span.Slice(sizeof(long), sizeof(long)));
        result.tid = MemoryMarshal.Read<long>(span.Slice(sizeof(long)*2, sizeof(long)));
        result.type = (LogType)MemoryMarshal.Read<int>(span.Slice(sizeof(long)*3, sizeof(int)));
        if (result.type == LogType.Write){
            result.keyAttr = KeyAttr.FromBytes(span.Slice(sizeof(long)*3+sizeof(int), KeyAttr.Size).ToArray(), tables);
            result.val = span.Slice(Size).ToArray();
        }

        return result;
        // LogEntry result = new LogEntry();
        // using (MemoryStream m = new MemoryStream(data)) {
        //     using (BinaryReader reader = new BinaryReader(m)) {
        //         result.lsn = reader.ReadInt64();
        //         result.prevLsn = reader.ReadInt64();
        //         result.tid = reader.ReadInt64();
        //         result.type = (LogType)reader.ReadInt32();
        //         if (result.type == LogType.Write){
        //             result.keyAttr = KeyAttr.FromBytes(reader.ReadBytes(KeyAttr.Size), tables);
        //             int valLength = reader.ReadInt32();
        //             result.val = reader.ReadBytes(valLength);
        //         }
        //     }
        // }
        // return result;
    }

    public override readonly bool Equals(object? obj)
    {
        if (obj == null || GetType() != obj.GetType())
        {
            return false;
        }
        LogEntry o = (LogEntry)obj;
        return o.lsn == lsn && o.prevLsn == prevLsn && o.tid == tid && o.type == type && o.keyAttr.Equals(keyAttr) && val.AsSpan().SequenceEqual(o.val);
    }

    public override readonly string ToString(){
        return $"LogEntry(lsn={lsn}, prevLsn={prevLsn}, tid={tid}, type={type}, keyAttr={keyAttr}, val={val})";
    }
    
}
}