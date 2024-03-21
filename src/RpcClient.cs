using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;


namespace DB {
public class RpcClient {
    private long partitionId;
    private Dictionary<long, GrpcChannel> clusterMap;

    public RpcClient(long partitionId, Dictionary<long, GrpcChannel> clusterMap){
        this.partitionId = partitionId;
        this.clusterMap = clusterMap;
    }

    public long GetId(){
        return partitionId;
    }
    public ReadOnlySpan<byte> Read(PrimaryKey key, TransactionContext ctx){
        var channel = GetServerChannel(key);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);
        var reply = client.Read(new ReadRequest { Keys = {key.Keys}, Table = key.Table, Tid = ctx.tid, PartitionId = partitionId});
        
        return reply.Value.ToByteArray();
    }

    /// <summary>
    /// Returns the appropriate channel to talk to correct shard
    /// </summary>
    /// <param name="key"></param>
    /// <returns>Null if key maps to itself, appropriate channel otherwise</returns>
    private GrpcChannel? GetServerChannel(PrimaryKey key){
        var id = HashKeyToDarqId(key);
        if (id == partitionId) return null;
        return clusterMap[id];
    }

    // TODO: arbitrary for now, define some rules for how to map keys to servers
    public long HashKeyToDarqId(PrimaryKey key){
        // uncomment for YCSB
        return key.Keys[0] % clusterMap.Count;
        // uncomment for TPCC
        // return key.Keys[0];
    }

    public bool IsLocalKey(PrimaryKey key){
        return HashKeyToDarqId(key) == partitionId;
    }

    public int GetNumServers(){
        return clusterMap.Count();
    }
}
}