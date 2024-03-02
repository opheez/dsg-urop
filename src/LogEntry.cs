
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
    public KeyAttr[] keyAttrs;
    public byte[][] vals;
    public static readonly int MinSize = sizeof(long) * 3 + sizeof(int);
    public LogEntry(long prevLsn, long tid, KeyAttr keyAttr, byte[] val){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = LogType.Write;
        this.keyAttrs = new KeyAttr[1]{keyAttr};
        this.vals = new byte[][]{val};
    }
    public LogEntry(long prevLsn, long tid, KeyAttr[] keyAttr, byte[][] val){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = LogType.Write;
        this.keyAttrs = keyAttr;
        this.vals = val;
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
        // todo: accurate size
        int totalSize = MinSize + (vals != null ? (sizeof(int) * (keyAttrs.Length + 1)) + Util.GetLength(vals) + (KeyAttr.Size * keyAttrs.Length) : 0);

        byte[] arr = new byte[totalSize];

        // Using MemoryMarshal to write the fixed-size fields to the byte array
        Span<byte> span = arr.AsSpan();
        MemoryMarshal.Write(span, ref lsn);
        MemoryMarshal.Write(span.Slice(sizeof(long)), ref prevLsn);
        MemoryMarshal.Write(span.Slice(sizeof(long)*2), ref tid);
        int typeAsInt = (int)type;
        MemoryMarshal.Write(span.Slice(sizeof(long)*3), ref typeAsInt);
        
        // Write the variable-sized byte array to the byte array
        if (type == LogType.Write || type == LogType.Prepare){
            int len = keyAttrs.Length;
            MemoryMarshal.Write(span.Slice(MinSize), ref len);
            int offset = MinSize + sizeof(int);
            for (int i = 0; i < keyAttrs.Length; i++){
                keyAttrs[i].ToBytes().CopyTo(span.Slice(offset));
                offset += KeyAttr.Size;
                int valLen = vals[i].Length;
                MemoryMarshal.Write(span.Slice(offset), ref valLen);
                offset += sizeof(int);
                vals[i].CopyTo(span.Slice(offset));
                offset += vals[i].Length;
            }
        }

        return arr;
    }

    public static LogEntry FromBytes(byte[] data, Dictionary<int, Table> tables) {
        // Ensure that the data array has enough bytes for the struct
        if (data.Length < MinSize) throw new ArgumentException("Insufficient data to deserialize the struct.");

        LogEntry result = new LogEntry();

        // Using MemoryMarshal to read the fixed-size fields from the byte array
        Span<byte> span = data.AsSpan();
        result.lsn = MemoryMarshal.Read<long>(span.Slice(0, sizeof(long)));
        result.prevLsn = MemoryMarshal.Read<long>(span.Slice(sizeof(long), sizeof(long)));
        result.tid = MemoryMarshal.Read<long>(span.Slice(sizeof(long)*2, sizeof(long)));
        result.type = (LogType)MemoryMarshal.Read<int>(span.Slice(sizeof(long)*3, sizeof(int)));

        if (result.type == LogType.Write || result.type == LogType.Prepare){
            int len = MemoryMarshal.Read<int>(span.Slice(MinSize, sizeof(int)));
            result.keyAttrs = new KeyAttr[len];
            result.vals = new byte[len][];
            int offset = MinSize + sizeof(int);
            for (int i = 0; i < len; i++){
                result.keyAttrs[i] = KeyAttr.FromBytes(span.Slice(offset, KeyAttr.Size).ToArray(), tables);
                offset += KeyAttr.Size;
                int valLen = MemoryMarshal.Read<int>(span.Slice(offset, sizeof(int)));
                offset += sizeof(int);
                result.vals[i] = span.Slice(offset, valLen).ToArray();
                offset += valLen;
            }
        }

        return result;
    }

    public override readonly bool Equals(object? obj)
    {
        if (obj == null || GetType() != obj.GetType())
        {
            return false;
        }
        LogEntry o = (LogEntry)obj;
        if (vals.Length != o.vals.Length) return false;
        for (int i = 0; i < vals.Length; i++){
            if (!vals[i].AsSpan().SequenceEqual(o.vals[i])) return false;
        }
        return o.lsn == lsn && o.prevLsn == prevLsn && o.tid == tid && o.type == type && o.keyAttrs.SequenceEqual(keyAttrs);
    }

    public override readonly string ToString(){
        return $"LogEntry(lsn={lsn}, prevLsn={prevLsn}, tid={tid}, type={type}, keyAttr={keyAttrs}, val={vals})";
    }
    
}
}