// using System.Text;
// using FASTER.client;
// using FASTER.core;
// using FASTER.darq;
// using FASTER.libdpr;
// using System.Diagnostics;
// using FASTER.common;
// using Grpc.Net.Client;
// using darq;
// using darq.client;
// using System.Collections.Concurrent;
// using Google.Protobuf;

// namespace DB {

// // Takes in requests for stored procedures and executes them
// public class DarqProcessor : IDarqProcessor {
//     private Darq backend;
//     private readonly DarqBackgroundTask _backgroundTask;
//     private readonly DarqBackgroundWorkerPool workerPool;
//     private readonly ManualResetEventSlim terminationStart, terminationComplete;
//     private Thread refreshThread, processingThread;
//     private ColocatedDarqProcessorClient<RwLatchVersionScheme> processorClient;

//     private IDarqProcessorClientCapabilities capabilities;

//     private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
//     private int nextWorker = 0;

//     /// previously
//     // private IDarqProcessorClientCapabilities capabilities;
//     // private DarqId me;
//     private StepRequest reusableRequest = new();
//     private IWriteAheadLog wal;
//     Dictionary<int, Table> tables = new Dictionary<int, Table>();

    
//     public DarqProcessor(Darq darq, DarqBackgroundWorkerPool workerPool, Dictionary<DarqId, GrpcChannel> clusterMap){
//         backend = darq;
//         _backgroundTask = new DarqBackgroundTask(backend, workerPool, session => new TransactionProcessorProducerWrapper(clusterMap, session));
//         terminationStart = new ManualResetEventSlim();
//         terminationComplete = new ManualResetEventSlim();
//         this.workerPool = workerPool;
//         backend.ConnectToCluster();
        
//         _backgroundTask.StopProcessing();

//         refreshThread = new Thread(() =>
//         {
//             while (!terminationStart.IsSet)
//                 backend.Refresh();
//             terminationComplete.Set();
//         });
//         refreshThread.Start();

//         processorClient = new ColocatedDarqProcessorClient<RwLatchVersionScheme>(backend);
//         processingThread = new Thread(() =>
//         {
//             processorClient.StartProcessing(this);
//         });
//         processingThread.Start();
//         // TODO(Tianyu): Hacky
//         // spin until we are sure that we have started 
//         while (capabilities == null) {}
//     }

//     public Darq GetBackend() => backend;

//     public void Dispose(){
//         terminationStart.Set();
//         // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
//         backend.ForceCheckpoint();
//         Thread.Sleep(1000);
//         _backgroundTask.StopProcessing();
//         _backgroundTask.Dispose();
//         processorClient.StopProcessingAsync().GetAwaiter().GetResult();
//         processorClient.Dispose();
//         terminationComplete.Wait();
//         refreshThread.Join();
//         processingThread.Join();
//     }

//     public bool ProcessMessage(DarqMessage m){
//         Console.WriteLine($"Processing message");
//         bool recoveryMode = false;
//         switch (m.GetMessageType()){
//             case DarqMessageType.IN:
//             {
//                 unsafe
//                 {
//                     fixed (byte* b = m.GetMessageBody())
//                     {
//                         int signal = *(int*)b;
//                         // This is a special termination signal
//                         if (signal == -1)
//                         {
//                             m.Dispose();
//                             // Return false to signal that there are no more messages to process and the processing
//                             // loop can exit
//                             return false;
//                         }
//                     }
//                 }

//                 LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray(), tables);
//                 switch (entry.type)
//                 {
//                     case LogType.Prepare:
//                         Console.WriteLine($"Got prepare log entry: {entry}");
//                         break;
//                     case LogType.Commit:
//                         Console.WriteLine($"Got commit log entry: {entry}");
//                         // write self message
                        
//                         break;
//                     default:
//                         throw new NotImplementedException();
//                 }

//                 var requestBuilder = new StepRequestBuilder(reusableRequest);
//                 requestBuilder.MarkMessageConsumed(m.GetLsn());
//                 requestBuilder.AddRecoveryMessage(m.GetMessageBody());
//                 m.Dispose();
//                 var v = capabilities.Step(requestBuilder.FinishStep());
//                 Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
//                 return true;
//             }
//             case DarqMessageType.RECOVERY: // this is on recovery; TODO: do we need to double pass?
//                 Console.WriteLine($"Recovering?, got log");
//                 if (recoveryMode) {
//                     LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray(), tables);
                    
//                     Console.WriteLine($"Recovering, got log entry: {entry}");

//                 }
//                 m.Dispose();
//                 return true;
//             default:
//                 throw new NotImplementedException();
//         }
//     }

//     public void SetWal(DARQWal wal) {
//         this.wal = wal;
//     }

//     public void OnRestart(IDarqProcessorClientCapabilities capabilities) {
//         this.capabilities = capabilities;
//         this.wal.SetCapabilities(capabilities);
//     }
// }

// public class TransactionProcessorProducerWrapper : IDarqProducer
// {
//     private Dictionary<DarqId, GrpcChannel> clusterMap;
//     private ConcurrentDictionary<DarqId, TransactionProcessor.TransactionProcessorClient> clients = new();
//     private DprSession session;

//     public TransactionProcessorProducerWrapper(Dictionary<DarqId, GrpcChannel> clusterMap, DprSession session)
//     {
//         this.clusterMap = clusterMap;
//         this.session = session;
//     }
    
//     public void Dispose() {}

//     public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback, long producerId, long lsn)
//     {
//         var client = clients.GetOrAdd(darqId,
//             _ => new TransactionProcessor.TransactionProcessorClient(clusterMap[darqId]));
//         var walRequest = new WalRequest
//         {
//             Message = ByteString.CopyFrom(message),
//             ProducerId = producerId,
//             Lsn = lsn
//         };
//         Task.Run(async () =>
//         {
//             try
//             {
//                 await client.WriteWalEntryAsync(walRequest);
//                 callback(true);
//             }
//             catch
//             {
//                 callback(false);
//                 throw;
//             }
//         });
//     }

//     public void ForceFlush()
//     {
//         // TODO(Tianyu): Not implemented for now
//     }
// }
// }