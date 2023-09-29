using FastSerialization;

namespace DB {

public struct LogEntry{
    long transactionId;
    Operation op;
    public LogEntry(long id, Operation op){
        transactionId = id;
        this.op = op;
    }

    public byte[] ToBytes(){
        using (MemoryStream m = new MemoryStream()) {
            using (BinaryWriter writer = new BinaryWriter(m)) {
                writer.Write(transactionId);
                byte[] opBytes = op.ToBytes();
                writer.Write(opBytes.Length);
                writer.Write(opBytes);
            }
            return m.ToArray();
        }
    }

    public static LogEntry FromBytes(byte[] data) {
        LogEntry result = new LogEntry();
        using (MemoryStream m = new MemoryStream(data)) {
            using (BinaryReader reader = new BinaryReader(m)) {
                result.transactionId = reader.ReadInt64();
                int opBytesLength = reader.ReadInt32();
                result.op = Operation.FromBytes(reader.ReadBytes(opBytesLength));
            }
        }
        return result;
    }
    
}
}