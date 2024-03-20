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

public enum WarehouseField{
    W_ID,
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    W_TAX,
    W_YTD
}
public enum DistricField{
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
    D_NEXT_O_ID
}
public enum CustomerField{
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
    C_DATA
}
public enum HistoryField{
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT,
    H_DATA
}
public enum NewOrderField{
    NO_O_ID,
    NO_D_ID,
    NO_W_ID
}
public enum OrderField{
    O_ID,
    O_D_ID,
    O_W_ID,
    O_C_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL
}
public enum OrderLineField{
    OL_O_ID,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DIST_INFO
}
public enum ItemField{
    I_ID,
    I_IM_ID,
    I_NAME,
    I_PRICE,
    I_DATA
}
public enum StockField{
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
    // public const 
    public static (long, int, Type)[] WAREHOUSE_SCHEMA = new (long, int, Type)[]{
        // ((long)WarehouseField.W_ID, 8, typeof(long)),
        ((long)WarehouseField.W_NAME, 10, typeof(string)),
        ((long)WarehouseField.W_STREET_1, 20, typeof(string)),
        ((long)WarehouseField.W_STREET_2, 20, typeof(string)),
        ((long)WarehouseField.W_CITY, 20, typeof(string)),
        ((long)WarehouseField.W_STATE, 2, typeof(string)),
        ((long)WarehouseField.W_ZIP, 9, typeof(string)),
        ((long)WarehouseField.W_TAX, 4, typeof(float)),
        ((long)WarehouseField.W_YTD, 4, typeof(float))
    };

    public static (long, int, Type)[] DISTRICT_SCHEMA = new (long, int, Type)[]{
        // ((long)DistricField.D_ID, 1, typeof(byte)),
        // ((long)DistricField.D_W_ID, 8, typeof(long)),
        ((long)DistricField.D_NAME, 10, typeof(string)),
        ((long)DistricField.D_STREET_1, 20, typeof(string)),
        ((long)DistricField.D_STREET_2, 20, typeof(string)),
        ((long)DistricField.D_CITY, 20, typeof(string)),
        ((long)DistricField.D_STATE, 2, typeof(string)),
        ((long)DistricField.D_ZIP, 9, typeof(string)),
        ((long)DistricField.D_TAX, 4, typeof(float)),
        ((long)DistricField.D_YTD, 4, typeof(float)),
        ((long)DistricField.D_NEXT_O_ID, 4, typeof(int))
    };

    public static (long, int, Type)[] CUSTOMER_SCHEMA = new (long, int, Type)[]{
        // ((long)CustomerField.C_ID, 4, typeof(int)),
        // ((long)CustomerField.C_D_ID, 1, typeof(byte)),
        // ((long)CustomerField.C_W_ID, 8, typeof(long)),
        ((long)CustomerField.C_FIRST, 16, typeof(string)),
        ((long)CustomerField.C_MIDDLE, 2, typeof(string)),
        ((long)CustomerField.C_LAST, 16, typeof(string)),
        ((long)CustomerField.C_STREET_1, 20, typeof(string)),
        ((long)CustomerField.C_STREET_2, 20, typeof(string)),
        ((long)CustomerField.C_CITY, 20, typeof(string)),
        ((long)CustomerField.C_STATE, 2, typeof(string)),
        ((long)CustomerField.C_ZIP, 9, typeof(string)),
        ((long)CustomerField.C_PHONE, 16, typeof(string)),
        ((long)CustomerField.C_SINCE, 8, typeof(DateTime)),
        ((long)CustomerField.C_CREDIT, 2, typeof(string)),
        ((long)CustomerField.C_CREDIT_LIM, 4, typeof(float)),
        ((long)CustomerField.C_DISCOUNT, 4, typeof(float)),
        ((long)CustomerField.C_BALANCE, 4, typeof(float)),
        ((long)CustomerField.C_YTD_PAYMENT, 4, typeof(float)),
        ((long)CustomerField.C_PAYMENT_CNT, 4, typeof(int)),
        ((long)CustomerField.C_DELIVERY_CNT, 4, typeof(int)),
        ((long)CustomerField.C_DATA, 500, typeof(string))
    };

    public static (long, int, Type)[] HISTORY_SCHEMA = new (long, int, Type)[]{
        // ((long)HistoryField.H_C_ID, 4, typeof(int)),
        // ((long)HistoryField.H_C_D_ID, 1, typeof(byte)),
        // ((long)HistoryField.H_C_W_ID, 8, typeof(long)),
        // ((long)HistoryField.H_D_ID, 1, typeof(byte)),
        // ((long)HistoryField.H_W_ID, 8, typeof(long)),
        // ((long)HistoryField.H_DATE, 8, typeof(DateTime)),
        ((long)HistoryField.H_AMOUNT, 4, typeof(float)),
        ((long)HistoryField.H_DATA, 24, typeof(string))
    };

    public static (long, int, Type)[] NEW_ORDER_SCHEMA = new (long, int, Type)[]{
        // ((long)NewOrderField.NO_O_ID, 4, typeof(int)),
        // ((long)NewOrderField.NO_D_ID, 1, typeof(byte)),
        // ((long)NewOrderField.NO_W_ID, 8, typeof(long))
    };

    public static (long, int, Type)[] ORDER_SCHEMA = new (long, int, Type)[]{
        // ((long)OrderField.O_ID, 4, typeof(int)),
        // ((long)OrderField.O_D_ID, 1, typeof(byte)),
        // ((long)OrderField.O_W_ID, 8, typeof(long)),
        ((long)OrderField.O_C_ID, 4, typeof(int)),
        ((long)OrderField.O_ENTRY_D, 8, typeof(DateTime)),
        ((long)OrderField.O_CARRIER_ID, 1, typeof(byte)),
        ((long)OrderField.O_OL_CNT, 4, typeof(int)),
        ((long)OrderField.O_ALL_LOCAL, 4, typeof(int))
    };

    public static (long, int, Type)[] ORDER_LINE_SCHEMA = new (long, int, Type)[]{
        // ((long)OrderLineField.OL_O_ID, 4, typeof(int)),
        // ((long)OrderLineField.OL_D_ID, 1, typeof(byte)),
        // ((long)OrderLineField.OL_W_ID, 8, typeof(long)),
        // ((long)OrderLineField.OL_NUMBER, 4, typeof(int)),
        ((long)OrderLineField.OL_I_ID, 4, typeof(int)),
        ((long)OrderLineField.OL_SUPPLY_W_ID, 4, typeof(int)),
        ((long)OrderLineField.OL_DELIVERY_D, 8, typeof(DateTime)),
        ((long)OrderLineField.OL_QUANTITY, 4, typeof(int)),
        ((long)OrderLineField.OL_AMOUNT, 4, typeof(float)),
        ((long)OrderLineField.OL_DIST_INFO, 24, typeof(string))
    };

    public static (long, int, Type)[] ITEM_SCHEMA = new (long, int, Type)[]{
        // ((long)ItemField.I_ID, 4, typeof(int)),
        ((long)ItemField.I_IM_ID, 4, typeof(int)),
        ((long)ItemField.I_NAME, 24, typeof(string)),
        ((long)ItemField.I_PRICE, 4, typeof(float)),
        ((long)ItemField.I_DATA, 50, typeof(string))
    };

    public static (long, int, Type)[] STOCK_SCHEMA = new (long, int, Type)[]{
        // ((long)StockField.S_ID, 4, typeof(int)),
        // ((long)StockField.S_W_ID, 8, typeof(long)),
        ((long)StockField.S_QUANTITY, 4, typeof(int)),
        ((long)StockField.S_DIST_01, 24, typeof(string)),
        ((long)StockField.S_DIST_02, 24, typeof(string)),
        ((long)StockField.S_DIST_03, 24, typeof(string)),
        ((long)StockField.S_DIST_04, 24, typeof(string)),
        ((long)StockField.S_DIST_05, 24, typeof(string)),
        ((long)StockField.S_DIST_06, 24, typeof(string)),
        ((long)StockField.S_DIST_07, 24, typeof(string)),
        ((long)StockField.S_DIST_08, 24, typeof(string)),
        ((long)StockField.S_DIST_09, 24, typeof(string)),
        ((long)StockField.S_DIST_10, 24, typeof(string)),
        ((long)StockField.S_YTD, 4, typeof(int)),
        ((long)StockField.S_ORDER_CNT, 4, typeof(int)),
        ((long)StockField.S_REMOTE_CNT, 4, typeof(int)),
        ((long)StockField.S_DATA, 50, typeof(string))
    };
}


}

