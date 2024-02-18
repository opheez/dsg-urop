using Grpc.Net.Client;
using FASTER.client;
using FASTER.libdpr;


namespace DB {
public class RpcClient {
    public const string DomainAddress = "http://127.0.0.1";
    public const int BasePort = 50050;
    public WorkerId me;
    private IDarqClusterInfo clusterInfo;
    private Dictionary<WorkerId, GrpcChannel> channelMap;

    public RpcClient(WorkerId me, IDarqClusterInfo clusterInfo){
        this.clusterInfo = clusterInfo;
        this.me = me;
        // create channel to each server
        channelMap = new Dictionary<WorkerId, GrpcChannel>();
        foreach (var server in clusterInfo.GetWorkers()){
            WorkerId workerId = server.Item1;
            string address = DomainAddress + ":" + BasePort + workerId.guid;
            channelMap[workerId] = GrpcChannel.ForAddress(address);
            Console.WriteLine($"Created channel to {address}");
        }

    }
    public ReadOnlySpan<byte> Read(long key){
        var channel = GetServerChannel(key);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);
        var reply = client.Read(new ReadRequest { Key = key });
        return reply.Value.ToByteArray();
    }

    /// <summary>
    /// Returns the appropriate channel to talk to correct shard
    /// </summary>
    /// <param name="key"></param>
    /// <returns>Null if key maps to itself, appropriate channel otherwise</returns>
    private GrpcChannel? GetServerChannel(long key){
        var workerId = HashKeyToWorkerId(key);
        Console.WriteLine($"Hashing key {key} to worker id {workerId.guid}");
        if (workerId.Equals(me)) return null;
        return channelMap[workerId];
    }

    // TODO: arbitrary for now, define some rules for how to map keys to servers
    private WorkerId HashKeyToWorkerId(long key){
        return new WorkerId((int)(key % clusterInfo.GetNumWorkers()));
    }
}
}