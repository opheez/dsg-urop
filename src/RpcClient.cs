using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;
using System.Collections.Concurrent;


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
        PbPrimaryKey pk = new PbPrimaryKey { Keys = {key.Keys}, Table = key.Table};

        var reply = client.Read(new ReadRequest { Key = pk, Tid = ctx.tid, PartitionId = partitionId});
        
        return reply.Value.ToByteArray();
    }

    public (byte[], PrimaryKey) ReadSecondary(PrimaryKey tempPk, byte[] key, TransactionContext ctx){
        var channel = GetServerChannel(tempPk);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);

        var reply = client.ReadSecondary(
            new ReadSecondaryRequest { 
                Key = ByteString.CopyFrom(key),
                Table = tempPk.Table,
                Tid = ctx.tid,
                PartitionId = partitionId
            }
        );
        return (reply.Value.ToByteArray(), new PrimaryKey(tempPk.Table, reply.Key.Keys.ToArray()));
    }

    public void SetSecondaryIndex(PrimaryKey tempPk, ConcurrentDictionary<byte[], PrimaryKey> index){
        var channel = GetServerChannel(tempPk);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);

        ByteString[] secondaryKeys = new ByteString[index.Count];
        PbPrimaryKey[] primaryKeys = new PbPrimaryKey[index.Count];
        int i = 0;
        foreach (var kv in index){
            secondaryKeys[i] = ByteString.CopyFrom(kv.Key);
            primaryKeys[i] = new PbPrimaryKey { Keys = {kv.Value.Keys}, Table = kv.Value.Table};
            i++;
        }

        client.SetSecondary(
            new SetSecondaryRequest {
                Keys = {secondaryKeys},
                Values = {primaryKeys},
                Table = tempPk.Table
            }
        );
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
        // return key.Keys[0] % clusterMap.Count;
        // uncomment for TPCC
        if (key.Table == (int)TableType.Item) return partitionId;
        return key.Keys[0] - 1;
    }

    public bool IsLocalKey(PrimaryKey key){
        if (key.Table == (int)TableType.Item) return true;
        return HashKeyToDarqId(key) == partitionId;
    }

    public int GetNumServers(){
        return clusterMap.Count();
    }
}
}