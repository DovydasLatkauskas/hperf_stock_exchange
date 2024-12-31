@0xeed8900d07222ffd;

struct BuyOrderPublic {
    orderAmount @0 :Int32;
    unitPriceCents @1 :Int32;
}

struct SellOrderPublic {
    orderAmount @0 :Int32;
    unitPriceCents @1 :Int32;
}

struct StockIdOrderListsTuple{
    stockId @0 :Text;
    buyOrders @1 :List(BuyOrderPublic);
    sellOrders @2 :List(SellOrderPublic);
}

struct BroadcastStruct {
    orders @0 :List(StockIdOrderListsTuple);
}