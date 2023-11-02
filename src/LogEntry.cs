
using System.Runtime.Serialization.Formatters.Binary;
using FASTER.core;

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
    Operation op;
    public LogEntry(long prevLsn, long tid, Operation op){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.op = op;
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
                    writer.Write((int)LogType.Write);
                    byte[] opBytes = op.ToBytes();
                    writer.Write(opBytes.Length);
                    writer.Write(opBytes);
                }
            }
            return m.ToArray();
        }
    }

    public static LogEntry FromBytes(byte[] data) {
        LogEntry result = new LogEntry();
        using (MemoryStream m = new MemoryStream(data)) {
            using (BinaryReader reader = new BinaryReader(m)) {
                result.lsn = reader.ReadInt64();
                result.prevLsn = reader.ReadInt64();
                result.tid = reader.ReadInt64();
                result.type = (LogType)reader.ReadInt32();
                if (result.type == LogType.Write){
                    int opBytesLength = reader.ReadInt32();
                    result.op = Operation.FromBytes(reader.ReadBytes(opBytesLength));
                }
            }
        }
        return result;
    }

    public override readonly string ToString(){
        return $"LogEntry(lsn={lsn}, prevLsn={prevLsn}, tid={tid}, type={type}, op={op})";
    }
    
}
}