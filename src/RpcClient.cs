using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;


namespace DB {
public class RpcClient {
    public const string DomainAddress = "http://127.0.0.1";
    public const int BasePort = 50050;
    private DarqId me;
    private IDarqClusterInfo clusterInfo;
    private Dictionary<DarqId, GrpcChannel> channelMap;

    public RpcClient(DarqId me, IDarqClusterInfo clusterInfo){
        this.clusterInfo = clusterInfo;
        this.me = me;
        // create channel to each server
        channelMap = new Dictionary<DarqId, GrpcChannel>();
        foreach (var server in clusterInfo.GetMembers()){
            DarqId darqId = server.Item1;
            string address = DomainAddress + ":" + BasePort + darqId.guid;
            channelMap[darqId] = GrpcChannel.ForAddress(address);
            Console.WriteLine($"Created channel to {address}");
        }

    }

    public DarqId GetDarqId(){
        return me;
    }
    public ReadOnlySpan<byte> Read(long key, TransactionContext ctx){
        var channel = GetServerChannel(key);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);
        var reply = client.Read(new ReadRequest { Key = key, Tid = ctx.tid, Me = me.guid});
        return reply.Value.ToByteArray();
    }

    /// <summary>
    /// Returns the appropriate channel to talk to correct shard
    /// </summary>
    /// <param name="key"></param>
    /// <returns>Null if key maps to itself, appropriate channel otherwise</returns>
    private GrpcChannel? GetServerChannel(long key){
        var darqId = HashKeyToDarqId(key);
        Console.WriteLine($"Hashing key {key} to worker id {darqId.guid}");
        if (darqId.Equals(me)) return null;
        return channelMap[darqId];
    }

    // TODO: arbitrary for now, define some rules for how to map keys to servers
    public DarqId HashKeyToDarqId(long key){
        return new DarqId((int)(key % clusterInfo.GetClusterSize()));
    }
}
}