using System.Text;
using DB;
using SharpNeat.Utility;

public struct TpccConfig {
    public int NumWh;
    public int NumDistrict;

    public TpccConfig(int numWh = 2, int numDistrict = 10){
        NumWh = numWh;
        NumDistrict = numDistrict;
    }
}

/// <summary>
/// Adapted from https://github.com/SQLServerIO/TPCCBench/blob/master/TPCCDatabaseGenerator/TPCCGenData.cs
/// and coco
/// </summary>
public class TpccBenchmark {

    private static readonly FastRandom Frnd = new FastRandom();
    private static byte[] RandHold = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
    private static byte[] ZipRandHold = Encoding.ASCII.GetBytes("1234567890");
    private static byte SpaceAsByte = (byte)' ';
    public int PartitionId;
    private TpccConfig cfg;
    private Dictionary<int, Table> tables;
    private TransactionManager txnManager;

    public TpccBenchmark(int partitionId, TpccConfig cfg, Dictionary<int, Table> tables, TransactionManager txnManager){
        PartitionId = partitionId;
        this.cfg = cfg;
        this.tables = tables;
        this.txnManager = txnManager;
    }

    public void Run(){
        foreach (TableType tableType in Enum.GetValues(typeof(TableType)))
        {
            PopulateTable(tableType, tables[(int)tableType]);
        }
    }

    public void PopulateTable(TableType tableType, Table table){
        TransactionContext ctx = txnManager.Begin();
        switch (tableType) 
        {
            case TableType.Warehouse:
                PopulateWarehouseTable(tables[(int)tableType], ctx);
                break;
            case TableType.District:
                PopulateDistrictTable(tables[(int)tableType], ctx);
                break;
            case TableType.Customer:
                PopulateCustomerTable(tables[(int)tableType], ctx);
                break;
            case TableType.History:
                PopulateHistoryTable(tables[(int)tableType], ctx);
                break;
            case TableType.NewOrder:
                PopulateNewOrderTable(tables[(int)tableType], ctx);
                break;
            case TableType.Order:
                PopulateOrderTable(tables[(int)tableType], ctx);
                break;
            case TableType.OrderLine:
                PopulateOrderLineTable(tables[(int)tableType], ctx);
                break;
            case TableType.Item:
                PopulateItemTable(tables[(int)tableType], ctx);
                break;
            case TableType.Stock:
                PopulateStockTable(tables[(int)tableType], ctx);
                break;
            default:
                throw new ArgumentException("Invalid table type");
        }
        txnManager.Commit(ctx);
    }
    public void PopulateWarehouseTable(Table table, TransactionContext ctx){
        // each partition has a single warehouse
        byte[] data = new byte[table.rowSize];
        Span<byte> span = new Span<byte>(data);
        
        int offset = 0;
        RandomByteString(6, 10).CopyTo(span); // W_NAME
        offset += 10;
        RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_STREET_1
        offset += 20;
        RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_STREET_2
        offset += 20;
        RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_CITY
        offset += 20;
        RandomByteString(2, 2).CopyTo(span.Slice(offset)); // W_STATE
        offset += 2;
        RandZip().CopyTo(span.Slice(offset)); // W_ZIP
        offset += 9;
        BitConverter.GetBytes(0.1000).CopyTo(span.Slice(offset)); // W_TAX
        offset += 4;
        BitConverter.GetBytes(3000000.00).CopyTo(span.Slice(offset)); // W_YTD
        table.Insert(new PrimaryKey(table.GetId(), PartitionId), table.GetSchema(), data, ctx);
        // PK: W_ID
    }
    public void PopulateDistrictTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            byte[] data = new byte[table.rowSize];
            Span<byte> span = new Span<byte>(data);
            
            int offset = 0;
            RandomByteString(6, 10).CopyTo(span.Slice(offset)); // D_NAME
            offset += 10;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_STREET_1
            offset += 20;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_STREET_2
            offset += 20;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_CITY
            offset += 20;
            RandomByteString(2, 2).CopyTo(span.Slice(offset)); // D_STATE
            offset += 2;
            RandZip().CopyTo(span.Slice(offset)); // D_ZIP
            offset += 9;
            RandFloat(0, 2000, 10000).CopyTo(span.Slice(offset)); // D_TAX
            offset += 4;
            BitConverter.GetBytes(30000).CopyTo(span.Slice(offset)); // D_YTD
            offset += 8;
            BitConverter.GetBytes(3001).CopyTo(span.Slice(offset)); // D_NEXT_O_ID
            table.Insert(new PrimaryKey(table.GetId(), PartitionId, i), table.GetSchema(), data, ctx);
            // PK: D_W_ID, D_ID
        }
    }
    public void PopulateCustomerTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            for (int j = 1; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                RandomByteString(8, 16).CopyTo(span.Slice(offset)); // C_FIRST
                offset += 16;
                new byte[]{(byte)'O', (byte)'E'}.CopyTo(span.Slice(offset)); // C_MIDDLE
                offset += 2;
                RandomByteString(8, 16).CopyTo(span.Slice(offset)); // C_LAST; TODO: implement correctly
                offset += 16;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_STREET_1
                offset += 20;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_STREET_2
                offset += 20;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_CITY
                offset += 20;
                RandomByteString(2, 2).CopyTo(span.Slice(offset)); // C_STATE
                offset += 2;
                RandZip().CopyTo(span.Slice(offset)); // C_ZIP
                offset += 9;
                RandomByteString(16, 16).CopyTo(span.Slice(offset)); // C_PHONE
                offset += 16;
                BitConverter.GetBytes(DateTime.Now.ToBinary()).CopyTo(span.Slice(offset)); // C_SINCE
                offset += 8;
                new byte[]{(byte)(Frnd.Next(0, 10) == 1 ? 'B' : 'G'), (byte)'C'}.CopyTo(span.Slice(offset)); // C_CREDIT
                offset += 2;
                BitConverter.GetBytes(50000).CopyTo(span.Slice(offset)); // C_CREDIT_LIM
                offset += 4;
                RandFloat(0, 5000, 10000).CopyTo(span.Slice(offset)); // C_DISCOUNT
                offset += 4;
                BitConverter.GetBytes(-10).CopyTo(span.Slice(offset)); // C_BALANCE
                offset += 4;
                BitConverter.GetBytes(10).CopyTo(span.Slice(offset)); // C_YTD_PAYMENT
                offset += 4;
                BitConverter.GetBytes(1).CopyTo(span.Slice(offset)); // C_PAYMENT_CNT
                offset += 4;
                BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // C_DELIVERY_CNT
                offset += 4;
                RandomByteString(300, 500).CopyTo(span.Slice(offset)); // C_DATA
                table.Insert(new PrimaryKey(table.GetId(), PartitionId, i, j), table.GetSchema(), data, ctx);
                // PK: C_W_ID, C_D_ID, C_ID
            }
        }
    }
    public void PopulateHistoryTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            for (int j = 1; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                BitConverter.GetBytes(10).CopyTo(span.Slice(offset)); // H_AMOUNT
                offset += 4;
                RandomByteString(12, 24).CopyTo(span.Slice(offset)); // H_DATA
                table.Insert(new PrimaryKey(table.GetId(), PartitionId, i, PartitionId, i, j, DateTime.Now.ToBinary()), table.GetSchema(), data, ctx);
                // PK: H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID, H_DATE
            }
        }
    }
    public void PopulateNewOrderTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            for (int j = 2101; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                table.Insert(new PrimaryKey(table.GetId(), PartitionId, i, j), table.GetSchema(), data, ctx);
                // PK: NO_W_ID, NO_D_ID, NO_O_ID
            }
        }
    }
    public void PopulateOrderTable(Table table, TransactionContext ctx){
        int[] cids = new int[3000];
        Random rand = new Random();
        for (int i = 1; i <= 3000; i++) {
            cids[i-1] = i;
        }
        
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            // TODO: shuffle cid
            for (int j = 1; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                BitConverter.GetBytes(cids[j-1]).CopyTo(span.Slice(offset)); // O_C_ID
                offset += 4;
                BitConverter.GetBytes(DateTime.Now.ToBinary()).CopyTo(span.Slice(offset)); // O_ENTRY_D
                offset += 8;
                BitConverter.GetBytes(j < 2101 ? Frnd.Next(1,10) : 0).CopyTo(span.Slice(offset)); // O_CARRIER_ID
                offset += 4;
                BitConverter.GetBytes(Frnd.Next(5,15)).CopyTo(span.Slice(offset)); // O_OL_CNT
                offset += 4;
                BitConverter.GetBytes(true).CopyTo(span.Slice(offset)); // O_ALL_LOCAL
                table.Insert(new PrimaryKey(table.GetId(), PartitionId, i, j), table.GetSchema(), data, ctx);
                // PK: O_W_ID, O_D_ID, O_ID
            }
        }
    }

    public void PopulateOrderLineTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= cfg.NumDistrict; i++)
        {
            for (int j = 1; j <= 3000; j++)
            {
                PrimaryKey pk = new PrimaryKey((int)TableType.Order, PartitionId, i, j);
                byte[] val = tables[(int)TableType.Order].Read(pk).ToArray();
                int olCnt = BitConverter.ToInt32(val, 13);
                DateTime oEntryD = DateTime.FromBinary(BitConverter.ToInt64(val, 4));

                for (int k = 1; k <= olCnt; k++)
                {
                    byte[] data = new byte[table.rowSize];
                    Span<byte> span = new Span<byte>(data);
                    
                    int offset = 0;
                    BitConverter.GetBytes(Frnd.Next(1,100000)).CopyTo(span.Slice(offset)); // OL_I_ID
                    offset += 4;
                    BitConverter.GetBytes(PartitionId).CopyTo(span.Slice(offset)); // OL_SUPPLY_W_ID
                    offset += 8;
                    BitConverter.GetBytes(j < 2101 ? oEntryD.ToBinary() : 0).CopyTo(span.Slice(offset)); // OL_DELIVERY_D
                    offset += 8;
                    BitConverter.GetBytes(5).CopyTo(span.Slice(offset)); // OL_QUANTITY
                    offset += 4;
                    BitConverter.GetBytes(j < 2101 ? 0 : Frnd.Next(1, 999999) / 100).CopyTo(span.Slice(offset)); // OL_AMOUNT
                    offset += 4;
                    RandomByteString(24, 24).CopyTo(span.Slice(offset)); // OL_DIST_INFO
                    table.Insert(new PrimaryKey(table.GetId(), PartitionId, i, j, k), table.GetSchema(), data, ctx);
                    // PK: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
                }
            }
        }
    }
    public void PopulateItemTable(Table table, TransactionContext ctx){
        for (int i = 1; i <= 100000; i++)
        {
            byte[] data = new byte[table.rowSize];
            Span<byte> span = new Span<byte>(data);
            
            int offset = 0;
            BitConverter.GetBytes(Frnd.Next(1,10000)).CopyTo(span.Slice(offset)); // I_IM_ID
            offset += 4;
            RandomByteString(14, 24).CopyTo(span.Slice(offset)); // I_NAME
            offset += 24;
            BitConverter.GetBytes(Frnd.Next(1,100)).CopyTo(span.Slice(offset)); // I_PRICE
            offset += 4;
            RandomByteString(26, 50).CopyTo(span.Slice(offset)); // I_DATA; TODO: coco spec
            table.Insert(new PrimaryKey(table.GetId(), i), table.GetSchema(), data, ctx);
            // PK: I_ID
        }
    }
    public void PopulateStockTable(Table table, TransactionContext ctx) {
        for (int i = 1; i <= 100000; i++)
        {

            byte[] data = new byte[table.rowSize];
            Span<byte> span = new Span<byte>(data);
            
            int offset = 0;
            BitConverter.GetBytes(Frnd.Next(10,100)).CopyTo(span.Slice(offset)); // S_QUANTITY
            offset += 4;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_01
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_02
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_03
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_04
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_05
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_06
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_07
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_08
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_09
            offset += 24;
            RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_10
            offset += 24;
            BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_YTD
            offset += 4;
            BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_ORDER_CNT
            offset += 4;
            BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_REMOTE_CNT
            offset += 4;
            RandomByteString(26, 50).CopyTo(span.Slice(offset)); // S_DATA; TODO: coco spec
            table.Insert(new PrimaryKey(table.GetId(), PartitionId, i), table.GetSchema(), data, ctx);
            // PK: S_W_ID, S_I_ID
        }
    }
    
    /// <summary>
    /// Generates a random byte array with the given length, 
    /// padded with spaces until it reaches the maximum length 
    /// </summary>
    /// <param name="strMin">
    ///   minimum size of the string
    /// </param>
    /// <param name="strMax"></param>
    /// <returns>Random string</returns>
    private static byte[] RandomByteString(int strMin, int strMax)
    {
        byte[] randomString = new byte[strMax];
        int stringLen = Frnd.Next(strMin, strMax);
        for (int x = 0; x < strMax; ++x)
        {
            if (x < stringLen)
                randomString[x] = RandHold[Frnd.Next(0, 62)];
            else
                randomString[x] = SpaceAsByte;
        }

        return randomString;
    }

    /// <summary>
    /// Generates a random zip code byte array with the given length
    /// </summary>
    /// <returns>Random string</returns>
    private static byte[] RandZip()
    {
        byte[] holdZip = new byte[5];
        for (int x = 0; x < 4; ++x)
        {
            holdZip[x] = ZipRandHold[Frnd.Next(0, 9)];
        }
        for (int x = 4; x < 5; ++x)
        {
            holdZip[x] = (byte)'1';
        }

        return holdZip;
    }

    /// <summary>
    /// Generates a 8 byte array representation of a random float 
    /// </summary>
    /// <param name="min">min value random</param>
    /// <param name="max">max value random</param>
    /// <param name="divisor">divisor</param>
    /// <returns>8 byte</returns>
    private static byte[] RandFloat(int min, int max, int divisor){
        return BitConverter.GetBytes(Frnd.Next(min, max) / divisor);
    }

}