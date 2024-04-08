using System.Collections.Concurrent;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography.Xml;
using System.Text;
using DB;
using SharpNeat.Utility;

public struct TpccConfig {
    public int NumWh;
    public int NumDistrict;
    public int NumItem;
    public int NewOrderCrossPartitionProbability;
    public int PaymentCrossPartitionProbability;

    public TpccConfig(int numWh = 2, int numDistrict = 10, int numItem = 100000, int newOrderCrossPartitionProbability = 10, int paymentCrossPartitionProbability = 15){
        NumWh = numWh;
        NumDistrict = numDistrict;
        NumItem = numItem;
        NewOrderCrossPartitionProbability = newOrderCrossPartitionProbability;
        PaymentCrossPartitionProbability = paymentCrossPartitionProbability;
    }
}

public struct NewOrderQuery : Query {
    public int w_id;
    public int d_id;
    public int c_id;
    public int o_ol_cnt;
    public int[] ol_i_ids;
    public int[] ol_supply_w_id;
    public int[] ol_quantity;
    public NewOrderQuery(int w_id, int d_id, int c_id, int o_ol_cnt, int[] ol_i_ids, int[] ol_supply_w_id, int[] ol_quantity){
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.o_ol_cnt = o_ol_cnt;
        this.ol_i_ids = ol_i_ids;
        this.ol_supply_w_id = ol_supply_w_id;
        this.ol_quantity = ol_quantity;
    }

    public unsafe byte[] ToBytes(){
        byte[] bytes = new byte[4 + 4 + 4 + 4 + 4 + 4 + 4 * ol_i_ids.Length + 4 * ol_supply_w_id.Length + 4 * ol_quantity.Length];
        fixed (byte* b = bytes){
            *(int*)b = w_id;
            *(int*)(b + 4) = d_id;
            *(int*)(b + 8) = c_id;
            *(int*)(b + 12) = o_ol_cnt;
            *(int*)(b + 16) = ol_i_ids.Length;
            for (int i = 0; i < ol_i_ids.Length; i++){
                *(int*)(b + 20 + i * 4) = ol_i_ids[i];
            }
            *(int*)(b + 20 + ol_i_ids.Length * 4) = ol_supply_w_id.Length;
            for (int i = 0; i < ol_supply_w_id.Length; i++){
                *(int*)(b + 24 + ol_i_ids.Length * 4 + i * 4) = ol_supply_w_id[i];
            }
            *(int*)(b + 24 + ol_i_ids.Length * 4 + ol_supply_w_id.Length * 4) = ol_quantity.Length;
            for (int i = 0; i < ol_quantity.Length; i++){
                *(int*)(b + 28 + ol_i_ids.Length * 4 + ol_supply_w_id.Length * 4 + i * 4) = ol_quantity[i];
            }
        }
        return bytes;
    }

    public static unsafe NewOrderQuery FromBytes(byte[] bytes){
        int w_id, d_id, c_id, o_ol_cnt;
        int[] ol_i_ids, ol_supply_w_id, ol_quantity;
        fixed (byte* b = bytes){
            w_id = *(int*)b;
            d_id = *(int*)(b + 4);
            c_id = *(int*)(b + 8);
            o_ol_cnt = *(int*)(b + 12);
            int ol_i_ids_len = *(int*)(b + 16);
            ol_i_ids = new int[ol_i_ids_len];
            for (int i = 0; i < ol_i_ids_len; i++){
                ol_i_ids[i] = *(int*)(b + 20 + i * 4);
            }
            int ol_supply_w_id_len = *(int*)(b + 20 + ol_i_ids_len * 4);
            ol_supply_w_id = new int[ol_supply_w_id_len];
            for (int i = 0; i < ol_supply_w_id_len; i++){
                ol_supply_w_id[i] = *(int*)(b + 24 + ol_i_ids_len * 4 + i * 4);
            }
            int ol_quantity_len = *(int*)(b + 24 + ol_i_ids_len * 4 + ol_supply_w_id_len * 4);
            ol_quantity = new int[ol_quantity_len];
            for (int i = 0; i < ol_quantity_len; i++){
                ol_quantity[i] = *(int*)(b + 28 + ol_i_ids_len * 4 + ol_supply_w_id_len * 4 + i * 4);
            }
        }
        return new NewOrderQuery(w_id, d_id, c_id, o_ol_cnt, ol_i_ids, ol_supply_w_id, ol_quantity);
    }
}

public struct PaymentQuery : Query {
    public int w_id;
    public int d_id;
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public float h_amount;
    public string c_last;
    public static int Size = 4 + 4 + 4 + 4 + 4 + 4 + 16;
    public PaymentQuery(int w_id, int d_id, int c_id, int c_d_id, int c_w_id, float h_amount, string c_last){
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.c_d_id = c_d_id;
        this.c_w_id = c_w_id;
        this.h_amount = h_amount;
        this.c_last = c_last;
    }

    public unsafe byte[] ToBytes(){
        byte[] bytes = new byte[Size];
        fixed (byte* b = bytes){
            *(int*)b = w_id;
            *(int*)(b + 4) = d_id;
            *(int*)(b + 8) = c_id;
            *(int*)(b + 12) = c_d_id;
            *(int*)(b + 16) = c_w_id;
            *(float*)(b + 20) = h_amount;
            Encoding.ASCII.GetBytes(c_last).CopyTo(bytes, 24);
        }
        return bytes;
    }

    public static unsafe PaymentQuery FromBytes(byte[] bytes){
        int w_id, d_id, c_id, c_d_id, c_w_id;
        float h_amount;
        string c_last;
        fixed (byte* b = bytes){
            w_id = *(int*)b;
            d_id = *(int*)(b + 4);
            c_id = *(int*)(b + 8);
            c_d_id = *(int*)(b + 12);
            c_w_id = *(int*)(b + 16);
            h_amount = *(float*)(b + 20);
            c_last = Encoding.ASCII.GetString(bytes, 24, 16);
        }
        return new PaymentQuery(w_id, d_id, c_id, c_d_id, c_w_id, h_amount, c_last);
    }

}

public interface Query {
    public byte[] ToBytes();

}

/// <summary>
/// Adapted from https://github.com/SQLServerIO/TPCCBench/blob/master/TPCCDatabaseGenerator/TPCCGenData.cs
/// and coco
/// </summary>
public class TpccBenchmark : TableBenchmark {

    private static readonly FastRandom Frnd = new FastRandom();
    private static byte[] RandHold = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
    private static byte[] ZipRandHold = Encoding.ASCII.GetBytes("1234567890");
    private static byte SpaceAsByte = (byte)' ';
    private static string[] LastNames = new string[] {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    private static byte[] ORIGINAL = Encoding.ASCII.GetBytes("ORIGINAL");
    private static string ItemDataFilename = "itemData.bin";

    public int PartitionId;
    private TpccConfig tpcCfg;
    private Dictionary<int, Table> tables;
    private TransactionManager txnManager;
    Query[] queries;

    public TpccBenchmark(int partitionId, TpccConfig tpcCfg, BenchmarkConfig cfg, Dictionary<int, Table> tables, TransactionManager txnManager) : base(cfg){
        System.Console.WriteLine("Init");
        PartitionId = partitionId;
        this.tpcCfg = tpcCfg;
        this.tables = tables;
        this.txnManager = txnManager;

        int numNewOrders = GenerateQueryData(PartitionId, "");

        stats = new BenchmarkStatistics($"TpccBenchmark", cfg, numNewOrders, cfg.datasetSize);
        System.Console.WriteLine("Done init");
    }

    private NewOrderQuery GenerateNewOrderQuery(int partitionId){
        int rbk = Frnd.Next(1, 100);
        int w_id = partitionId + 1;
        int o_ol_cnt = Frnd.Next(5, 15);
        int[] ol_i_ids = new int[o_ol_cnt];
        int[] ol_supply_w_id = new int[o_ol_cnt];
        int[] ol_quantity = new int[o_ol_cnt];
        for (int j = 0; j < o_ol_cnt; j++){
            bool retry;
            do {
                retry = false;
                ol_i_ids[j] = NonUniformRandom(8191, 1, 100000);
                for (int k = 0; k < j; k++){
                    if (ol_i_ids[j] == ol_i_ids[k]){
                        retry = true;
                        break;
                    }
                }
            } while (retry);

            if (j == o_ol_cnt - 1 && rbk == 1){
                ol_i_ids[j] = 0;
            }

            ol_supply_w_id[j] = w_id;
            if (j == 0) {
                int x = Frnd.Next(1, 100);
                if (x <= tpcCfg.NewOrderCrossPartitionProbability) {
                    while (ol_supply_w_id[j] == w_id) {
                        ol_supply_w_id[j] = Frnd.Next(1, tpcCfg.NumWh + 1);
                    }
                }
            }
            ol_quantity[j] = Frnd.Next(1, 10);
        }
        return new NewOrderQuery(
            w_id,
            Frnd.Next(1, tpcCfg.NumDistrict + 1), 
            NonUniformRandom(1023, 1, 3000),
            o_ol_cnt,
            ol_i_ids,
            ol_supply_w_id,
            ol_quantity
        );
    }

    public PaymentQuery GeneratePaymentQuery(int partitionId) {
        int w_id = partitionId + 1;
        int d_id = Frnd.Next(1, tpcCfg.NumDistrict + 1);
        int c_w_id = w_id;
        int c_d_id;
        int x = Frnd.Next(1, 100);
        if (x <= tpcCfg.PaymentCrossPartitionProbability) {
            while (c_w_id == w_id) {
                c_w_id = Frnd.Next(1, tpcCfg.NumWh + 1);
            }
            c_d_id = Frnd.Next(1, tpcCfg.NumDistrict + 1);
        } else {
            c_d_id = d_id;
        }

        int y = Frnd.Next(1, 100);
        string c_last = "";
        int c_id;
        if (y <= 60) {
            c_last = RandLastName();
            c_id = 0;
        } else {
            c_id = NonUniformRandom(1023, 1, 3000);
        }
        return new PaymentQuery(
            w_id,
            d_id,
            c_id,
            c_d_id,
            c_w_id,
            Frnd.Next(1, 5000),
            c_last
        );
    }

    public int GenerateQueryData(int partitionId, string filename) {
        this.queries = new Query[cfg.datasetSize];
        int numNewOrders = 0;
        // using (var writer = new BinaryWriter(File.Open(filename, FileMode.Create))) {
            for (int i = 0; i < queries.Length; i++){
                // randomly assign NewOrder vs Payment
                if (Frnd.Next(1, 100) <= 50){
                    numNewOrders++;
                    queries[i] = GenerateNewOrderQuery(partitionId);
                } else {
                    queries[i] = GeneratePaymentQuery(partitionId);
                }

                // byte[] bytes = queries[i].ToBytes();
                // writer.Write(bytes.Length);
                // writer.Write(bytes);
            }
        // }
        return numNewOrders;
    }

    private byte[] ExtractField(TableType tableType, TableField field, ReadOnlySpan<byte> row) {
        (int size, int offset) = tables[(int)tableType].GetAttrMetadata((long)field);
        return row.Slice(offset, size).ToArray();
    }

    private void SetField(TableType tableType, TableField field, byte[] row, byte[] value) {
        (int size, int offset) = tables[(int)tableType].GetAttrMetadata((long)field);
        value.CopyTo(row, offset);
    }

    private (byte[], TupleDesc[]) BuildUpdate(byte[] data, TupleDesc[] tds, TableType tableType, TableField field, byte[] value){
        int size = tables[(int)tableType].GetAttrMetadata((long)field).Item1;
        int offset = tds[tds.Length - 1].Offset + tds[tds.Length - 1].Size;
        return ((byte[])data.Concat(value), (TupleDesc[])tds.Append(new TupleDesc((int)field, size, offset)));
    }

    public void NewOrder(NewOrderQuery query){
        TransactionContext ctx = txnManager.Begin();
        ReadOnlySpan<byte> warehouseRow = tables[(int)TableType.Warehouse].Read(new PrimaryKey((int)TableType.Warehouse, query.w_id), tables[(int)TableType.Warehouse].GetSchema(), ctx);
        PrimaryKey districtPk = new PrimaryKey((int)TableType.District, query.w_id, query.d_id);
        ReadOnlySpan<byte> districtRow = tables[(int)TableType.District].Read(districtPk, tables[(int)TableType.District].GetSchema(), ctx);
        ReadOnlySpan<byte> customerRow = tables[(int)TableType.Customer].Read(new PrimaryKey((int)TableType.Customer, query.w_id, query.d_id, query.c_id), tables[(int)TableType.Customer].GetSchema(), ctx);

        byte[][] itemRows = new byte[query.o_ol_cnt][];
        byte[][] stockRows = new byte[query.o_ol_cnt][];
        bool allLocal = true;
        for (int i = 0; i < query.o_ol_cnt; i++)
        {
            if (query.ol_i_ids[i] == 0) txnManager.Abort(ctx);
            itemRows[i] = tables[(int)TableType.Item].Read(new PrimaryKey((int)TableType.Item, query.ol_i_ids[i]), tables[(int)TableType.Item].GetSchema(), ctx).ToArray();
            stockRows[i] = tables[(int)TableType.Stock].Read(new PrimaryKey((int)TableType.Stock, query.ol_supply_w_id[i], query.ol_i_ids[i]), tables[(int)TableType.Stock].GetSchema(), ctx).ToArray();
            if (query.ol_supply_w_id[i] != query.w_id) allLocal = false;
        }

        // update district with increment D_NEXT_O_ID
        byte[] old_d_next_o_id_bytes = ExtractField(TableType.District, TableField.D_NEXT_O_ID, districtRow);
        int new_d_next_i_id = BitConverter.ToInt32(old_d_next_o_id_bytes) + 1;
        byte[] new_d_next_i_id_bytes = BitConverter.GetBytes(new_d_next_i_id);
        tables[(int)TableType.District].Update(districtPk, new TupleDesc[]{new TupleDesc((int)TableField.D_NEXT_O_ID, 4, 0)}, new_d_next_i_id_bytes, ctx);

        // insert into order and new order
        PrimaryKey newOrderPk = new PrimaryKey((int)TableType.NewOrder, query.w_id, query.d_id, new_d_next_i_id);
        PrimaryKey orderPk = new PrimaryKey((int)TableType.Order, query.w_id, query.d_id, new_d_next_i_id);
        byte[] insertOrderData = new byte[tables[(int)TableType.Order].rowSize];
        SetField(TableType.Order, TableField.O_C_ID, insertOrderData, BitConverter.GetBytes(query.c_id));
        SetField(TableType.Order, TableField.O_ENTRY_D, insertOrderData, BitConverter.GetBytes(DateTime.Now.ToBinary()));
        SetField(TableType.Order, TableField.O_CARRIER_ID, insertOrderData, BitConverter.GetBytes(0));
        SetField(TableType.Order, TableField.O_OL_CNT, insertOrderData, BitConverter.GetBytes(query.o_ol_cnt));
        SetField(TableType.Order, TableField.O_ALL_LOCAL, insertOrderData, BitConverter.GetBytes(allLocal));
        tables[(int)TableType.Order].Insert(orderPk, tables[(int)TableType.Order].GetSchema(), insertOrderData, ctx);
        tables[(int)TableType.NewOrder].Insert(newOrderPk, tables[(int)TableType.NewOrder].GetSchema(), new byte[0], ctx);

        float total_amount = 0;
        for (int i = 0; i < query.o_ol_cnt; i++)
        {
            // update stock
            byte[] updateStockData = new byte[0];
            TupleDesc[] updateStockTds = new TupleDesc[0];
            float i_price = BitConverter.ToSingle(ExtractField(TableType.Item, TableField.I_PRICE, itemRows[i]));
            int s_quantity = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_QUANTITY, stockRows[i]));
            if (s_quantity >= query.ol_quantity[i] + 10) s_quantity -= query.ol_quantity[i];
            else s_quantity += 91 - query.ol_quantity[i];
            (updateStockData, updateStockTds) = BuildUpdate(updateStockData, updateStockTds, TableType.Stock, TableField.S_QUANTITY, BitConverter.GetBytes(s_quantity));
            int s_ytd = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_YTD, stockRows[i])) + query.ol_quantity[i];
            (updateStockData, updateStockTds) = BuildUpdate(updateStockData, updateStockTds, TableType.Stock, TableField.S_YTD, BitConverter.GetBytes(s_ytd));
            int s_order_cnt = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_ORDER_CNT, stockRows[i])) + 1;
            (updateStockData, updateStockTds) = BuildUpdate(updateStockData, updateStockTds, TableType.Stock, TableField.S_ORDER_CNT, BitConverter.GetBytes(s_order_cnt));
            if (query.ol_supply_w_id[i] != query.w_id)
            {
                int s_remote_cnt = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_REMOTE_CNT, stockRows[i])) + 1;
                (updateStockData, updateStockTds) = BuildUpdate(updateStockData, updateStockTds, TableType.Stock, TableField.S_REMOTE_CNT, BitConverter.GetBytes(s_remote_cnt));
            }
            tables[(int)TableType.Stock].Update(new PrimaryKey((int)TableType.Stock, query.ol_supply_w_id[i], query.ol_i_ids[i]), updateStockTds, updateStockData, ctx);

            // insert into order line
            float ol_amount = i_price * query.ol_quantity[i];
            byte[] updateOrderLineData = new byte[0];
            SetField(TableType.OrderLine, TableField.OL_I_ID, updateOrderLineData, BitConverter.GetBytes(query.ol_i_ids[i]));
            SetField(TableType.OrderLine, TableField.OL_SUPPLY_W_ID, updateOrderLineData, BitConverter.GetBytes(query.ol_supply_w_id[i]));
            SetField(TableType.OrderLine, TableField.OL_DELIVERY_D, updateOrderLineData, BitConverter.GetBytes(0));
            SetField(TableType.OrderLine, TableField.OL_QUANTITY, updateOrderLineData, BitConverter.GetBytes(query.ol_quantity[i]));
            SetField(TableType.OrderLine, TableField.OL_AMOUNT, updateOrderLineData, BitConverter.GetBytes(ol_amount));
            string distInfo = Encoding.ASCII.GetString(ExtractField(TableType.Stock, TableField.S_DIST_01 + query.d_id - 1, stockRows[i]));
            SetField(TableType.OrderLine, TableField.OL_DIST_INFO, updateOrderLineData, Encoding.ASCII.GetBytes(distInfo));
            tables[(int)TableType.OrderLine].Insert(new PrimaryKey((int)TableType.OrderLine, query.w_id, query.d_id, new_d_next_i_id, i), tables[(int)TableType.OrderLine].GetSchema(), updateOrderLineData, ctx);

            // update total_amount
            float c_discount = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_DISCOUNT, customerRow));
            float w_tax = BitConverter.ToSingle(ExtractField(TableType.Warehouse, TableField.W_TAX, warehouseRow));
            float d_tax = BitConverter.ToSingle(ExtractField(TableType.District, TableField.D_TAX, districtRow));
            total_amount += ol_amount * (1 - c_discount) * (1 + w_tax + d_tax);
        }
        txnManager.Commit(ctx);
    }

    public void Payment(PaymentQuery query){
        TransactionContext ctx = txnManager.Begin();
        PrimaryKey warehousePk = new PrimaryKey((int)TableType.Warehouse, query.w_id);
        ReadOnlySpan<byte> warehouseRow = tables[(int)TableType.Warehouse].Read(warehousePk, tables[(int)TableType.Warehouse].GetSchema(), ctx);
        PrimaryKey districtPk = new PrimaryKey((int)TableType.District, query.w_id, query.d_id);
        ReadOnlySpan<byte> districtRow = tables[(int)TableType.District].Read(districtPk, tables[(int)TableType.District].GetSchema(), ctx);
        if (query.c_id == 0) {
            byte[] secondaryIndexKey = BitConverter.GetBytes(query.c_w_id).Concat(BitConverter.GetBytes(query.c_d_id)).Concat(Encoding.ASCII.GetBytes(query.c_last)).ToArray();
            query.c_id = (int)tables[(int)TableType.Customer].PkFromSecondaryIndex(secondaryIndexKey).Keys[2];
        }

        PrimaryKey customerPk = new PrimaryKey((int)TableType.Customer, query.c_w_id, query.c_d_id, query.c_id);
        ReadOnlySpan<byte> customerRow = tables[(int)TableType.Customer].Read(customerPk, tables[(int)TableType.Customer].GetSchema(), ctx);

        // standard tpcc write to w_ytd
        // update warehouse with increment W_YTD
        float w_ytd = BitConverter.ToSingle(ExtractField(TableType.Warehouse, TableField.W_YTD, warehouseRow)) + query.h_amount;
        byte[] updateWarehouseData = new byte[0];
        TupleDesc[] updateWarehouseTds = new TupleDesc[0];
        (updateWarehouseData, updateWarehouseTds) = BuildUpdate(updateWarehouseData, updateWarehouseTds, TableType.Warehouse, TableField.W_YTD, BitConverter.GetBytes(w_ytd));
        tables[(int)TableType.Warehouse].Update(warehousePk, updateWarehouseTds, updateWarehouseData, ctx);

        // update district with increment D_YTD
        float d_ytd = BitConverter.ToSingle(ExtractField(TableType.District, TableField.D_YTD, districtRow)) + query.h_amount;
        byte[] updateDistrictData = new byte[0];
        TupleDesc[] updateDistrictTds = new TupleDesc[0];
        (updateDistrictData, updateDistrictTds) = BuildUpdate(updateDistrictData, updateDistrictTds, TableType.District, TableField.D_YTD, BitConverter.GetBytes(d_ytd));
        tables[(int)TableType.District].Update(districtPk, updateDistrictTds, updateDistrictData, ctx);

        // update customer
        byte[] updateCustomerData = new byte[0];
        TupleDesc[] updateCustomerTds = new TupleDesc[0];
        byte[] c_credit = ExtractField(TableType.Customer, TableField.C_CREDIT, customerRow);
        if (Encoding.ASCII.GetString(c_credit) == "BC")
        {
            string c_data_old = Encoding.ASCII.GetString(ExtractField(TableType.Customer, TableField.C_DATA, customerRow));
            string c_data_new = query.c_id + " " + query.c_d_id + " " + query.c_w_id + " " + query.d_id + " " + query.w_id + " " + String.Format("{0:0.00}", query.h_amount) + " " + c_data_old;
            c_data_new = c_data_new.Substring(0, Math.Min(c_data_new.Length, 500));
            (updateCustomerData, updateCustomerTds) = BuildUpdate(updateCustomerData, updateCustomerTds, TableType.Customer, TableField.C_DATA, Encoding.ASCII.GetBytes(c_data_new));
        }
        float c_balance = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_BALANCE, customerRow)) - query.h_amount;
        (updateCustomerData, updateCustomerTds) = BuildUpdate(updateCustomerData, updateCustomerTds, TableType.Customer, TableField.C_BALANCE, BitConverter.GetBytes(c_balance));
        float c_ytd_payment = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_YTD_PAYMENT, customerRow)) + query.h_amount;
        (updateCustomerData, updateCustomerTds) = BuildUpdate(updateCustomerData, updateCustomerTds, TableType.Customer, TableField.C_YTD_PAYMENT, BitConverter.GetBytes(c_ytd_payment));
        int c_payment_cnt = BitConverter.ToInt32(ExtractField(TableType.Customer, TableField.C_PAYMENT_CNT, customerRow)) + 1;
        (updateCustomerData, updateCustomerTds) = BuildUpdate(updateCustomerData, updateCustomerTds, TableType.Customer, TableField.C_PAYMENT_CNT, BitConverter.GetBytes(c_payment_cnt));
        tables[(int)TableType.Customer].Update(customerPk, updateCustomerTds, updateCustomerData, ctx);

        // insert into history
        string h_data = Encoding.ASCII.GetString(ExtractField(TableType.Warehouse, TableField.W_NAME, warehouseRow)) + "    " + Encoding.ASCII.GetString(ExtractField(TableType.District, TableField.D_NAME, districtRow));
        PrimaryKey historyPk = new PrimaryKey((int)TableType.History, query.w_id, query.d_id, query.c_w_id, query.c_d_id, query.c_id, DateTime.Now.ToBinary());
        byte[] insertHistoryData = new byte[tables[(int)TableType.History].rowSize];
        SetField(TableType.History, TableField.H_AMOUNT, insertHistoryData, BitConverter.GetBytes(query.h_amount));
        SetField(TableType.History, TableField.H_DATA, insertHistoryData, Encoding.ASCII.GetBytes(h_data));
        tables[(int)TableType.History].Insert(historyPk, tables[(int)TableType.History].GetSchema(), insertHistoryData, ctx);

        txnManager.Commit(ctx);
    }

    public void RunTransactions(){
        // init all tables
        GenerateItemData(tables[(int)TableType.Item], ItemDataFilename);
        for (int partitionId = 0; partitionId < tpcCfg.NumWh; partitionId++) {
            foreach (TableType tableType in Enum.GetValues(typeof(TableType)))
            {
                PopulateTable(tableType, tables[(int)tableType], partitionId);
            }
        }


        for (int i = 0; i < queries.Length; i++){
            if (queries[i] is NewOrderQuery){
                NewOrder((NewOrderQuery)queries[i]);
            } else {
                Payment((PaymentQuery)queries[i]);
            }
        }


    }

    public void PopulateTable(TableType tableType, Table table, int partitionId){
        TransactionContext ctx = txnManager.Begin();
        int w_id = partitionId + 1;
        switch (tableType) 
        {
            case TableType.Warehouse:
                PopulateWarehouseTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.District:
                PopulateDistrictTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.Customer:
                PopulateCustomerTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.History:
                PopulateHistoryTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.NewOrder:
                PopulateNewOrderTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.Order:
                PopulateOrderTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.OrderLine:
                PopulateOrderLineTable(tables[(int)tableType], ctx, w_id);
                break;
            case TableType.Item:
                PopulateItemTable(tables[(int)tableType], ctx, ItemDataFilename);
                break;
            case TableType.Stock:
                PopulateStockTable(tables[(int)tableType], ctx, w_id);
                break;
            default:
                throw new ArgumentException("Invalid table type");
        }
        txnManager.Commit(ctx);
    }
    public void PopulateWarehouseTable(Table table, TransactionContext ctx, int w_id){
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
        BitConverter.GetBytes(0.1000f).CopyTo(span.Slice(offset)); // W_TAX
        offset += 4;
        BitConverter.GetBytes(3000000.00f).CopyTo(span.Slice(offset)); // W_YTD
        table.Insert(new PrimaryKey(table.GetId(), w_id), table.GetSchema(), data, ctx);
        // PK: W_ID
    }
    public void PopulateDistrictTable(Table table, TransactionContext ctx, int w_id){
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
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
            table.Insert(new PrimaryKey(table.GetId(), w_id, i), table.GetSchema(), data, ctx);
            // PK: D_W_ID, D_ID
        }
    }
    public void PopulateCustomerTable(Table table, TransactionContext ctx, int w_id){
        ConcurrentDictionary<byte[], PrimaryKey> secondaryIndex = new ConcurrentDictionary<byte[], PrimaryKey>();
        // group rows by new index attribute 
        Dictionary<byte[], List<(PrimaryKey, byte[])>> groupByAttr = new Dictionary<byte[], List<(PrimaryKey, byte[])>>();
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
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
                byte[] lastName = Encoding.ASCII.GetBytes(RandLastName());
                lastName.CopyTo(span.Slice(offset)); // C_LAST
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
                PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, j);
                table.Insert(pk, table.GetSchema(), data, ctx);
                // PK: C_W_ID, C_D_ID, C_ID

                byte[] key = BitConverter.GetBytes(w_id).Concat(BitConverter.GetBytes(i)).Concat(lastName).ToArray();
                if (!groupByAttr.ContainsKey(key)){
                    groupByAttr[key] = new List<(PrimaryKey, byte[])>();
                }
                groupByAttr[key].Add((pk, data));
                

            }

            foreach (var entry in groupByAttr){
                List<(PrimaryKey, byte[])> sameLastNames = entry.Value;
                // sort by C_FIRST
                sameLastNames.Sort((a, b) => {
                    return Util.CompareArrays(a.Item2[0..16], b.Item2[0..16]);
                });

                secondaryIndex[entry.Key] = sameLastNames[(sameLastNames.Count - 1) / 2].Item1;
            }
        }
        table.SetSecondaryIndex(secondaryIndex);
    }
    public void PopulateHistoryTable(Table table, TransactionContext ctx, int w_id){
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
        {
            for (int j = 1; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                BitConverter.GetBytes(10).CopyTo(span.Slice(offset)); // H_AMOUNT
                offset += 4;
                RandomByteString(12, 24).CopyTo(span.Slice(offset)); // H_DATA
                table.Insert(new PrimaryKey(table.GetId(), w_id, i, w_id, i, j, DateTime.Now.ToBinary()), table.GetSchema(), data, ctx);
                // PK: H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID, H_DATE
            }
        }
    }
    public void PopulateNewOrderTable(Table table, TransactionContext ctx, int w_id){
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
        {
            for (int j = 2101; j <= 3000; j++)
            {
                byte[] data = new byte[table.rowSize];
                table.Insert(new PrimaryKey(table.GetId(), w_id, i, j), table.GetSchema(), data, ctx);
                // PK: NO_W_ID, NO_D_ID, NO_O_ID
            }
        }
    }
    public void PopulateOrderTable(Table table, TransactionContext ctx, int w_id){
        int[] cids = new int[3000];
        for (int i = 1; i <= 3000; i++) {
            cids[i-1] = i;
        }
        
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
        {
            Util.Shuffle(Frnd, cids);
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
                table.Insert(new PrimaryKey(table.GetId(), w_id, i, j), table.GetSchema(), data, ctx);
                // PK: O_W_ID, O_D_ID, O_ID
            }
        }
    }

    public void PopulateOrderLineTable(Table table, TransactionContext ctx, int w_id){
        for (int i = 1; i <= tpcCfg.NumDistrict; i++)
        {
            for (int j = 1; j <= 3000; j++)
            {
                PrimaryKey pk = new PrimaryKey((int)TableType.Order, w_id, i, j);
                byte[] val = tables[(int)TableType.Order].Read(pk, tables[(int)TableType.Order].GetSchema(), ctx).ToArray();
                int olCnt = BitConverter.ToInt32(val, 13);
                DateTime oEntryD = DateTime.FromBinary(BitConverter.ToInt64(val, 4));

                for (int k = 1; k <= olCnt; k++)
                {
                    byte[] data = new byte[table.rowSize];
                    Span<byte> span = new Span<byte>(data);
                    
                    int offset = 0;
                    BitConverter.GetBytes(Frnd.Next(1,100000)).CopyTo(span.Slice(offset)); // OL_I_ID
                    offset += 4;
                    BitConverter.GetBytes(w_id).CopyTo(span.Slice(offset)); // OL_SUPPLY_W_ID
                    offset += 8;
                    BitConverter.GetBytes(j < 2101 ? oEntryD.ToBinary() : 0).CopyTo(span.Slice(offset)); // OL_DELIVERY_D
                    offset += 8;
                    BitConverter.GetBytes(5).CopyTo(span.Slice(offset)); // OL_QUANTITY
                    offset += 4;
                    BitConverter.GetBytes(j < 2101 ? 0 : Frnd.Next(1, 999999) / 100f).CopyTo(span.Slice(offset)); // OL_AMOUNT
                    offset += 4;
                    RandomByteString(24, 24).CopyTo(span.Slice(offset)); // OL_DIST_INFO
                    table.Insert(new PrimaryKey(table.GetId(), w_id, i, j, k), table.GetSchema(), data, ctx);
                    // PK: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
                }
            }
        }
    }

    private void GenerateItemData(Table table, string filename){
        using (var writer = new BinaryWriter(File.Open(filename, FileMode.Create))) {
            for (int i = 1; i <= tpcCfg.NumItem; i++)
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
                byte[] i_data = RandomByteString(26, 50);
                int strLen = Encoding.ASCII.GetString(i_data).IndexOf(' ');
                if (Frnd.Next(1,10) == 1) {
                    int start = Frnd.Next(0, strLen - ORIGINAL.Length);
                    for (int j = 0; j < ORIGINAL.Length; j++) {
                        i_data[start + j] = ORIGINAL[j];
                    }
                }
                i_data.CopyTo(span.Slice(offset)); // I_DATA
                // PK: I_ID
                PrimaryKey pk = new PrimaryKey(table.GetId(), i);

                byte[] pkBytes = pk.ToBytes();
                writer.Write(pkBytes.Length);
                writer.Write(pkBytes);
                writer.Write(data);
            }
        }
    }

    public void PopulateItemTable(Table table, TransactionContext ctx, string filename){
        using (var reader = new BinaryReader(File.Open(filename, FileMode.Open))) {
            for (int i = 1; i <= tpcCfg.NumItem; i++)
            {
                int pkLen = reader.ReadInt32();
                byte[] pkBytes = reader.ReadBytes(pkLen);
                byte[] data = reader.ReadBytes(table.rowSize);
                table.Insert(PrimaryKey.FromBytes(pkBytes), table.GetSchema(), data, ctx);
            }
        }        
    }
    public void PopulateStockTable(Table table, TransactionContext ctx, int w_id) {
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
            byte[] s_data = RandomByteString(26, 50);
            int strLen = Encoding.ASCII.GetString(s_data).IndexOf(' ');
            if (Frnd.Next(1,10) == 1) {
                int start = Frnd.Next(0, strLen - ORIGINAL.Length);
                for (int j = 0; j < ORIGINAL.Length; j++) {
                    s_data[start + j] = ORIGINAL[j];
                }
            }
            s_data.CopyTo(span.Slice(offset)); // S_DATA
            table.Insert(new PrimaryKey(table.GetId(), w_id, i), table.GetSchema(), data, ctx);
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
        return BitConverter.GetBytes((float)Frnd.Next(min, max) / divisor);
    }

    /// <summary>
    /// Derived from coco's Random.h
    /// </summary>
    /// <param name="A"></param>
    /// <param name="C"></param>
    /// <param name="min"></param>
    /// <param name="max"></param>
    /// <returns></returns>
    private static int NonUniformRandom(int A, int min, int max){
        return ((Frnd.Next(0, A) | Frnd.Next(min, max)) % (max - min + 1)) + min;
    }

    private static string RandLastName(){
        int n = NonUniformRandom(255, 0, 999);
        return LastNames[n / 100] + LastNames[(n / 10) % 10] + LastNames[n % 10];
    }

}