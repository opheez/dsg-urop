namespace DB {
public enum TableType{
    Warehouse,
    Stock,
    District,
    Customer,
    History,
    Order,
    NewOrder,
    Item,
    OrderLine
}
public enum TableField{
    W_ID,
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    W_TAX,
    W_YTD,
    D_ID,
    D_W_ID,
    D_NAME,
    D_STREET_1,
    D_STREET_2,
    D_CITY,
    D_STATE,
    D_ZIP,
    D_TAX,
    D_YTD,
    D_NEXT_O_ID,
    C_ID,
    C_D_ID,
    C_W_ID,
    C_FIRST,
    C_MIDDLE,
    C_LAST,
    C_STREET_1,
    C_STREET_2,
    C_CITY,
    C_STATE,
    C_ZIP,
    C_PHONE,
    C_SINCE,
    C_CREDIT,
    C_CREDIT_LIM,
    C_DISCOUNT,
    C_BALANCE,
    C_YTD_PAYMENT,
    C_PAYMENT_CNT,
    C_DELIVERY_CNT,
    C_DATA,
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT,
    H_DATA,
    NO_O_ID,
    NO_D_ID,
    NO_W_ID,
    O_ID,
    O_D_ID,
    O_W_ID,
    O_C_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL,
    OL_O_ID,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DIST_INFO,
    I_ID,
    I_IM_ID,
    I_NAME,
    I_PRICE,
    I_DATA,
    S_ID,
    S_W_ID,
    S_QUANTITY,
    S_DIST_01,
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    S_YTD,
    S_ORDER_CNT,
    S_REMOTE_CNT,
    S_DATA
}

// value fields fit smallest data type possible for ids, despite all being converted to long
public static class TpccSchema {
    public static Dictionary<TableType, List<TableField>> tablesToFields = new Dictionary<TableType, List<TableField>> {
        {TableType.Warehouse, new List<TableField>{ TableField.W_NAME, TableField.W_STREET_1, TableField.W_STREET_2, TableField.W_CITY, TableField.W_STATE, TableField.W_ZIP, TableField.W_TAX, TableField.W_YTD}},
        {TableType.District, new List<TableField>{TableField.D_NAME, TableField.D_STREET_1, TableField.D_STREET_2, TableField.D_CITY, TableField.D_STATE, TableField.D_ZIP, TableField.D_TAX, TableField.D_YTD, TableField.D_NEXT_O_ID}},
        {TableType.Customer, new List<TableField>{TableField.C_FIRST, TableField.C_MIDDLE, TableField.C_LAST, TableField.C_STREET_1, TableField.C_STREET_2, TableField.C_CITY, TableField.C_STATE, TableField.C_ZIP, TableField.C_PHONE, TableField.C_SINCE, TableField.C_CREDIT, TableField.C_CREDIT_LIM, TableField.C_DISCOUNT, TableField.C_BALANCE, TableField.C_YTD_PAYMENT, TableField.C_PAYMENT_CNT, TableField.C_DELIVERY_CNT, TableField.C_DATA}},
        {TableType.History, new List<TableField>{TableField.H_AMOUNT, TableField.H_DATA}},
        {TableType.Order, new List<TableField>{TableField.O_C_ID, TableField.O_ENTRY_D, TableField.O_CARRIER_ID, TableField.O_OL_CNT, TableField.O_ALL_LOCAL}},
        {TableType.NewOrder, new List<TableField>()},
        {TableType.Item, new List<TableField>{TableField.I_ID, TableField.I_IM_ID, TableField.I_NAME, TableField.I_PRICE, TableField.I_DATA}},
        {TableType.OrderLine, new List<TableField>{TableField.OL_I_ID, TableField.OL_SUPPLY_W_ID, TableField.OL_DELIVERY_D, TableField.OL_QUANTITY, TableField.OL_AMOUNT, TableField.OL_DIST_INFO}},
        {TableType.Stock, new List<TableField>{TableField.S_QUANTITY, TableField.S_DIST_01, TableField.S_DIST_02, TableField.S_DIST_03, TableField.S_DIST_04, TableField.S_DIST_05, TableField.S_DIST_06, TableField.S_DIST_07, TableField.S_DIST_08, TableField.S_DIST_09, TableField.S_DIST_10, TableField.S_YTD, TableField.S_ORDER_CNT, TableField.S_REMOTE_CNT, TableField.S_DATA}}
    };
    public static Dictionary<TableField, (int, Type)> WAREHOUSE_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.W_ID, (8, typeof(long))},
        {TableField.W_NAME, (10, typeof(string))},
        {TableField.W_STREET_1, (20, typeof(string))},
        {TableField.W_STREET_2, (20, typeof(string))},
        {TableField.W_CITY, (20, typeof(string))},
        {TableField.W_STATE, (2, typeof(string))},
        {TableField.W_ZIP, (9, typeof(string))},
        {TableField.W_TAX, (4, typeof(float))},
        {TableField.W_YTD, (4, typeof(float))}
    };

    public static Dictionary<TableField, (int, Type)> DISTRICT_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.D_ID, (1, typeof(byte))},
        // {TableField.D_W_ID, (8, typeof(long))},
        {TableField.D_NAME, (10, typeof(string))},
        {TableField.D_STREET_1, (20, typeof(string))},
        {TableField.D_STREET_2, (20, typeof(string))},
        {TableField.D_CITY, (20, typeof(string))},
        {TableField.D_STATE, (2, typeof(string))},
        {TableField.D_ZIP, (9, typeof(string))},
        {TableField.D_TAX, (4, typeof(float))},
        {TableField.D_YTD, (4, typeof(float))},
        {TableField.D_NEXT_O_ID, (4, typeof(int))}
    };

    public static Dictionary<TableField, (int, Type)> CUSTOMER_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.C_ID, (4, typeof(int))},
        // {TableField.C_D_ID, (1, typeof(byte))},
        // {TableField.C_W_ID, (8, typeof(long))},
        {TableField.C_FIRST, (16, typeof(string))},
        {TableField.C_MIDDLE, (2, typeof(string))},
        {TableField.C_LAST, (16, typeof(string))},
        {TableField.C_STREET_1, (20, typeof(string))},
        {TableField.C_STREET_2, (20, typeof(string))},
        {TableField.C_CITY, (20, typeof(string))},
        {TableField.C_STATE, (2, typeof(string))},
        {TableField.C_ZIP, (9, typeof(string))},
        {TableField.C_PHONE, (16, typeof(string))},
        {TableField.C_SINCE, (8, typeof(DateTime))},
        {TableField.C_CREDIT, (2, typeof(string))},
        {TableField.C_CREDIT_LIM, (4, typeof(float))},
        {TableField.C_DISCOUNT, (4, typeof(float))},
        {TableField.C_BALANCE, (4, typeof(float))},
        {TableField.C_YTD_PAYMENT, (4, typeof(float))},
        {TableField.C_PAYMENT_CNT, (4, typeof(int))},
        {TableField.C_DELIVERY_CNT, (4, typeof(int))},
        {TableField.C_DATA, (500, typeof(string))}
    };

    public static Dictionary<TableField, (int, Type)> HISTORY_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.H_C_ID, (4, typeof(int))},
        // {TableField.H_C_D_ID, (1, typeof(byte))},
        // {TableField.H_C_W_ID, (8, typeof(long))},
        // {TableField.H_D_ID, (1, typeof(byte))},
        // {TableField.H_W_ID, (8, typeof(long))},
        // {TableField.H_DATE, (8, typeof(DateTime))},
        {TableField.H_AMOUNT, (4, typeof(float))},
        {TableField.H_DATA, (24, typeof(string))}
    };

    public static Dictionary<TableField, (int, Type)> NEW_ORDER_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.NO_O_ID, (4, typeof(int))},
        // {TableField.NO_D_ID, (1, typeof(byte))},
        // {TableField.NO_W_ID, (8, typeof(long))}
    };

    public static Dictionary<TableField, (int, Type)> ORDER_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.O_ID, (4, typeof(int))},
        // {TableField.O_D_ID, (1, typeof(byte))},
        // {TableField.O_W_ID, (8, typeof(long))},
        {TableField.O_C_ID, (4, typeof(long))},
        {TableField.O_ENTRY_D, (8, typeof(DateTime))},
        {TableField.O_CARRIER_ID, (1, typeof(byte))},
        {TableField.O_OL_CNT, (4, typeof(int))},
        {TableField.O_ALL_LOCAL, (4, typeof(int))}
    };

    public static Dictionary<TableField, (int, Type)> ORDER_LINE_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.OL_O_ID, (4, typeof(int))},
        // {TableField.OL_D_ID, (1, typeof(byte))},
        // {TableField.OL_W_ID, (8, typeof(long))},
        // {TableField.OL_NUMBER, (4, typeof(int))},
        {TableField.OL_I_ID, (4, typeof(int))},
        {TableField.OL_SUPPLY_W_ID, (4, typeof(int))},
        {TableField.OL_DELIVERY_D, (8, typeof(DateTime))},
        {TableField.OL_QUANTITY, (4, typeof(int))},
        {TableField.OL_AMOUNT, (4, typeof(float))},
        {TableField.OL_DIST_INFO, (24, typeof(string))}
    };

    public static Dictionary<TableField, (int, Type)> ITEM_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {ItemField.I_ID, (4, typeof(int))},
        {TableField.I_IM_ID, (4, typeof(int))},
        {TableField.I_NAME, (24, typeof(string))},
        {TableField.I_PRICE, (4, typeof(float))},
        {TableField.I_DATA, (50, typeof(string))}
    };

    public static Dictionary<TableField, (int, Type)> STOCK_SCHEMA = new Dictionary<TableField, (int, Type)>{
        // {TableField.S_ID, (4, typeof(int))},
        // {TableField.S_W_ID, (8, typeof(long))},
        {TableField.S_QUANTITY, (4, typeof(int))},
        {TableField.S_DIST_01, (24, typeof(string))},
        {TableField.S_DIST_02, (24, typeof(string))},
        {TableField.S_DIST_03, (24, typeof(string))},
        {TableField.S_DIST_04, (24, typeof(string))},
        {TableField.S_DIST_05, (24, typeof(string))},
        {TableField.S_DIST_06, (24, typeof(string))},
        {TableField.S_DIST_07, (24, typeof(string))},
        {TableField.S_DIST_08, (24, typeof(string))},
        {TableField.S_DIST_09, (24, typeof(string))},
        {TableField.S_DIST_10, (24, typeof(string))},
        {TableField.S_YTD, (4, typeof(int))},
        {TableField.S_ORDER_CNT, (4, typeof(int))},
        {TableField.S_REMOTE_CNT, (4, typeof(int))},
        {TableField.S_DATA, (50, typeof(string))}
    };

}

}