
// /// <summary>
// /// A user establishes a ClientSession when they want to communicate with the database. 
// /// This is done through transactions
// /// </summary>
// public class ClientSession<Key, Value> {

//     TransactionContext ctx;
//     Table<Key> tbl;
//     DBStore<Key> dbStore;

//     public ClientSession(Table<Key> table, DBStore<Key> dbStore){
//         ctx = new TransactionContext<Key, Value>();
//         tbl = table;
//         this.dbStore = dbStore;
//     }

//     public void Begin(){
        
//     }

//     public void Read(Key key, Key attr){
//         KeyAttr keyAttr = {key, attr};
//         // check if keyAttr is valid
//         // if ctx contains keyattr
//         //  return value
//         // otherwise
//         //  read value from table
//         //  add to transaction context
//     }

//     public void Upsert(Key key, Key attribute, Span<byte> value){
//         // check if keyAttr is valid
//         // add to transaction context
//     }

//     /// <summary>
//     /// Blocks on serializer
//     /// </summary>
//     public TransactionContext<Key, Value> Commit(){
//         // assert that ctx.status is idle
//         // add transaction context object (ctx) to serializer's queue
//         // while ctx.status is not done, wait
//         // if status is failed, try again 

//         // set ctx = new ctx
//         // return old ctx
//     }

//     public void Abort(){
//         // ctx = new ctx
//     }

// }