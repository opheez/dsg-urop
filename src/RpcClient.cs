using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;


namespace DB {
public class RpcClient {
    private long me;
    private Dictionary<long, GrpcChannel> clusterMap;

    public RpcClient(long me, Dictionary<long, GrpcChannel> clusterMap){
        this.me = me;
        this.clusterMap = clusterMap;
    }

    public long GetId(){
        return me;
    }
    public ReadOnlySpan<byte> Read(long key, TransactionContext ctx){
        var channel = GetServerChannel(key);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);
        var reply = client.Read(new ReadRequest { Key = key, Tid = ctx.tid, Me = me});
        return reply.Value.ToByteArray();
    }

    /// <summary>
    /// Returns the appropriate channel to talk to correct shard
    /// </summary>
    /// <param name="key"></param>
    /// <returns>Null if key maps to itself, appropriate channel otherwise</returns>
    private GrpcChannel? GetServerChannel(long key){
        var id = HashKeyToDarqId(key);
        Console.WriteLine($"Hashing key {key} to worker id {id}");
        if (id == me) return null;
        return clusterMap[id];
    }

    // TODO: arbitrary for now, define some rules for how to map keys to servers
    public long HashKeyToDarqId(long key){
        return key % clusterMap.Count();
    }
}
}