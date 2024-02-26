using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;


namespace DB {
public class RpcClient {
    public const string DomainAddress = "http://127.0.0.1";
    public const int BasePort = 50050;
    private long me;
    private Dictionary<long, GrpcChannel> channelMap;

    public RpcClient(long me, Dictionary<long, string> clusterInfo){
        this.me = me;
        // create channel to each server
        channelMap = new Dictionary<long, GrpcChannel>();
        foreach (var other in clusterInfo){
            if (other.Key == me) continue;
            channelMap[other.Key] = GrpcChannel.ForAddress(other.Value);
            Console.WriteLine($"Created channel to {other.Value}");
        }

    }

    public long GetDarqId(){
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
        return channelMap[id];
    }

    // TODO: arbitrary for now, define some rules for how to map keys to servers
    public long HashKeyToDarqId(long key){
        return key % channelMap.Count();
    }
}
}