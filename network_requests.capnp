@0xc4cafb10070b73a6;

struct MakeOrderRequest {
    requesterId @0 :Int32;
    stockId @1 :Text;
    orderAmount @2 :Int32;
    priceCents @3 :Int32;
    orderType @4 :OrderType;
}

enum OrderType {
  buy @0;
  sell @1;
}

struct CancelOrderRequest {
    requesterId @0 :Int32;
    stockId @1 :Text;
    orderType @2 :OrderType;
    cancelOrderId @3 :Data;
}

struct NetworkRequest {
    union {
        makeOrderRequest @0 :MakeOrderRequest;
        cancelOrderRequest @1 :CancelOrderRequest;
    }
}