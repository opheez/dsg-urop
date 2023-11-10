
using System.Runtime.Serialization.Formatters.Binary;

namespace DB {

public enum LogType {
    Begin,
    Commit,
    Abort,
    Write
}

public struct LogEntry{
    public long lsn;
    public long prevLsn; // do we even need this if we are undoing?
    public long tid;
    public LogType type;
    public KeyAttr keyAttr;
    public byte[] val;
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

    public byte[] ToBytes(){
        using (MemoryStream m = new MemoryStream()) {
            BinaryFormatter bf = new BinaryFormatter();
            using (BinaryWriter writer = new BinaryWriter(m)) {
                writer.Write(lsn);
                writer.Write(prevLsn);
                writer.Write(tid);
                writer.Write((int)type);
                if (type == LogType.Write){
                    writer.Write(keyAttr.ToBytes());
                    writer.Write(val.Length);
                    writer.Write(val);
                }
            }
            return m.ToArray();
        }
    }

    public static LogEntry FromBytes(byte[] data, Dictionary<int, Table> tables) {
        LogEntry result = new LogEntry();
        using (MemoryStream m = new MemoryStream(data)) {
            using (BinaryReader reader = new BinaryReader(m)) {
                result.lsn = reader.ReadInt64();
                result.prevLsn = reader.ReadInt64();
                result.tid = reader.ReadInt64();
                result.type = (LogType)reader.ReadInt32();
                if (result.type == LogType.Write){
                    result.keyAttr = KeyAttr.FromBytes(reader.ReadBytes(KeyAttr.Size), tables);
                    int valLength = reader.ReadInt32();
                    result.val = reader.ReadBytes(valLength);
                }
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
            return o.lsn == lsn && o.prevLsn == prevLsn && o.tid == tid && o.type == type && o.keyAttr.Equals(keyAttr) && val.AsSpan().SequenceEqual(o.val);
        }

        public override readonly string ToString(){
        return $"LogEntry(lsn={lsn}, prevLsn={prevLsn}, tid={tid}, type={type}, keyAttr={keyAttr}, val={val})";
    }
    
}
}