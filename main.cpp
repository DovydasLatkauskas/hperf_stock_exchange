#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <vector>
#include <queue>
#include <chrono>
#include <sys/epoll.h>
#include <fcntl.h>
#include <set>
#include <random>

#include <capnp/serialize-packed.h>
#include "network_requests.capnp.h"
#include "exchange_orders_broadcast.capnp.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace uuid = boost::uuids;

#define BROADCAST_PORT 4950
#define LISTEN_PORT 4951
#define BROADCAST_IP "255.255.255.255"

#define BROADCAST_TIMEOUT_MS 1000
#define MAXEVENTS 1 // open tcp connections + 1


struct BuyOrder {
    uint8_t orderId[16];
    int buyerId;
    mutable int orderAmount; // need mutability for order matching, make sure to not use in comparison overload
    int unitPriceCents;
    long orderAtUnix;

    bool operator<(const BuyOrder& other) const {
        if (unitPriceCents != other.unitPriceCents) // hot loop, can remove for performance
            return unitPriceCents > other.unitPriceCents;
        return orderAtUnix < other.orderAtUnix;
    }
};

struct SellOrder {
    uint8_t orderId[16];
    int sellerId;
    mutable int orderAmount; // need mutability for order matching, make sure to not use in comparison overload
    int unitPriceCents;
    long orderAtUnix;

    bool operator<(const SellOrder& other) const {
        if (unitPriceCents != other.unitPriceCents)
            return unitPriceCents < other.unitPriceCents;
        return orderAtUnix < other.orderAtUnix;
    }
};

// do dynamic size structs have any issues?
struct Orders {
    std::set<BuyOrder> buyOrders;
    std::set<SellOrder> sellOrders;
};

void broadcast_market_data(const int sockfd, const sockaddr_in *broadcast_addr,
                           std::unordered_map<std::string, Orders> *ordersPtr) {

    auto ordersMap = *ordersPtr;
    capnp::MallocMessageBuilder message;

    BroadcastStruct::Builder broadcastMsg = message.initRoot<BroadcastStruct>();
    capnp::List<StockIdOrderListsTuple>::Builder stocks = broadcastMsg.initOrders(ordersMap.size());

    int i = 0;
    for (const auto& pair : ordersMap) {
        StockIdOrderListsTuple::Builder tuple = stocks[i];
        tuple.setStockId(pair.first);
        auto sOrders = tuple.initSellOrders(pair.second.sellOrders.size());
        auto bOrders = tuple.initBuyOrders(pair.second.buyOrders.size());
        int j = 0;
        for (const auto& sellOrders : pair.second.sellOrders) {
            SellOrderPublic::Builder sop = sOrders[j];
            sop.setOrderAmount(sellOrders.orderAmount);
            sop.setUnitPriceCents(sellOrders.unitPriceCents);
            ++j;
        }
        j = 0;
        for (const auto& buyOrders : pair.second.buyOrders) {
            BuyOrderPublic::Builder bop = bOrders[j];
            bop.setOrderAmount(buyOrders.orderAmount);
            bop.setUnitPriceCents(buyOrders.unitPriceCents);
            ++j;
        }
        ++i;
    }

    kj::VectorOutputStream bufferStream;
    capnp::writePackedMessage(bufferStream, message); // TODO: benchmark against unpacked serialization
    auto serializedData = bufferStream.getArray();

    ssize_t bytesSent = sendto(sockfd, serializedData.begin(), serializedData.size(),
                               0, (struct sockaddr*)broadcast_addr, sizeof(*broadcast_addr));


    if (bytesSent == -1) {
        perror("sendto failed");
    } else {
        printf("Broadcasted %zd bytes.\n", bytesSent); // TODO: remove
    }

    if (serializedData.size() > 65507) { // 65535 - 8 (UDP header) - 20 (IP header) // TODO: what if there's more data than UDP supports?

        fprintf(stderr, "Packet size exceeds UDP limit: %zu bytes\n", serializedData.size());
        return;
    }
}

long currentUnixTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void generateRandomOrderId(uint8_t* id, size_t length) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (size_t i = 0; i < length; ++i) {
        id[i] = static_cast<uint8_t>(dis(gen));
    }
}

void populate_exchange_with_test_data(std::unordered_map<std::string, Orders> *orders,
                                      std::vector<std::string> *stockList) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> traderIdDist(100, 9999);
    std::uniform_int_distribution<> amountDist(1, 100);
    std::uniform_int_distribution<> buyPriceDist(500, 2000);
    std::uniform_int_distribution<> sellPriceDist(1900, 3000);


    for (const auto& stock : *stockList) {
        for (int i = 0; i < 25; ++i) {
            SellOrder sOrder;
            generateRandomOrderId(sOrder.orderId, 16);
            sOrder.sellerId = traderIdDist(gen);
            sOrder.orderAmount = amountDist(gen);
            sOrder.unitPriceCents = sellPriceDist(gen);
            sOrder.orderAtUnix = currentUnixTime();

            (*orders)[stock].sellOrders.insert(sOrder);
        }

        for (int i = 0; i < 25; ++i) {
            BuyOrder bOrder;
            generateRandomOrderId(bOrder.orderId, 16);
            bOrder.buyerId = traderIdDist(gen);
            bOrder.orderAmount = amountDist(gen);
            bOrder.unitPriceCents = buyPriceDist(gen);
            bOrder.orderAtUnix = currentUnixTime();

            (*orders)[stock].buyOrders.insert(bOrder);
        }
    }
}

// TODO: implement real
void notify_of_sale(std::set<SellOrder>::iterator sellOrder, std::set<BuyOrder>::iterator buyOrder,
                    int unitPriceCents, int amount, std::string *stockId) {
    std::cout << "Sale happened: " << amount << " units of " << *stockId
              << " at " << unitPriceCents << " cents per unit.\n"
              << "Seller ID: " << sellOrder->sellerId << ", Buyer ID: " << buyOrder->buyerId << std::endl;
}

void match_orders(std::unordered_map<std::string, Orders> *orders,
                  std::string *stock) {
    std::set<SellOrder> *sOrders = &(*orders)[*stock].sellOrders;
    std::set<BuyOrder> *bOrders = &(*orders)[*stock].buyOrders;
    auto bIt = (*bOrders).begin();
    auto sIt = (*sOrders).begin();

    while (sIt != (*sOrders).end() && bIt != (*bOrders).end()
        && sIt->unitPriceCents <= bIt->unitPriceCents) {
        int unitPriceCents = (sIt->orderAtUnix < bIt->orderAtUnix) ? sIt->unitPriceCents : bIt->unitPriceCents;

        if(sIt->orderAmount < bIt->orderAmount){
            notify_of_sale(sIt, bIt, unitPriceCents, sIt->orderAmount, stock);
            bIt->orderAmount -= sIt->orderAmount;
            sIt->orderAmount = 0;
        } else {
            notify_of_sale(sIt, bIt, unitPriceCents, bIt->orderAmount, stock);
            sIt->orderAmount -= bIt->orderAmount;
            bIt->orderAmount = 0;
        }

        if(bIt->orderAmount <= 0){
            bIt = (*bOrders).erase(bIt); // also iterates to the next one
        }
        if(sIt->orderAmount <= 0){
            sIt = (*sOrders).erase(sIt);
        }
    }
}

// returns whether a new order has been created and the stock
std::pair<bool, std::string> handleMakeOrderRequest(NetworkRequest::Reader *requestReaderPtr,
                                                    std::unordered_map<std::string, Orders> *orders) {
    auto req = (*requestReaderPtr).getMakeOrderRequest();

    if(req.getOrderType() == OrderType::BUY){
        BuyOrder order;
        order.unitPriceCents = req.getPriceCents();
        order.buyerId = req.getRequesterId();
        order.orderAmount = req.getOrderAmount();
        order.orderAtUnix = currentUnixTime();

        uuid::random_generator generator;
        uuid::uuid uuid = generator();
        std::copy(uuid.begin(), uuid.end(), order.orderId);

        (*orders)[req.getStockId()].buyOrders.insert(order);
        return std::make_pair(true, req.getStockId());
    } else {
        SellOrder order;
        order.unitPriceCents = req.getPriceCents();
        order.sellerId = req.getRequesterId();
        order.orderAmount = req.getOrderAmount();
        order.orderAtUnix = currentUnixTime();

        uuid::random_generator generator;
        uuid::uuid uuid = generator();
        std::copy(uuid.begin(), uuid.end(), order.orderId);

        (*orders)[req.getStockId()].sellOrders.insert(order);
        return std::make_pair(true, req.getStockId());
    }
}

// returns whether a new order has been created and the stock
std::pair<bool, std::string> handleCancelOrderRequest(NetworkRequest::Reader *requestReaderPtr,
                                                      std::unordered_map<std::string, Orders> *orders) {
    auto req = (*requestReaderPtr).getCancelOrderRequest();

    auto bytes = req.getCancelOrderId().asBytes();
    std::array<uint8_t, 16> cancelOrderId;
    std::copy(bytes.begin(), bytes.end(), cancelOrderId.begin());
    auto stockId = req.getStockId();

    auto sellOrders = &(*orders)[stockId].sellOrders;
    auto sIt = sellOrders->begin();
    auto sEnd = sellOrders->end();

    while(sIt != sEnd){
        if(std::equal(sIt->orderId, sIt->orderId + 16, cancelOrderId.begin())){
            sellOrders->erase(sIt);
            return std::make_pair(false, req.getStockId());
        }
    }

    auto buyOrders = &(*orders)[req.getStockId()].sellOrders;
    auto bIt = buyOrders->begin();
    auto bEnd = buyOrders->end();

    while(bIt != bEnd){
        if(std::equal(bIt->orderId, bIt->orderId + 16, cancelOrderId.begin())){
            buyOrders->erase(bIt);
            return std::make_pair(false, req.getStockId());
        }
    }

    std::cerr << "Order ID not found for cancellation: " << req.getStockId().cStr() << std::endl; // TODO: return error to caller
    return std::make_pair(false, req.getStockId());
}

int main() {
    // global declarations
    // matching engine data structures

    std::unordered_map<std::string, Orders> orders; // using a hashmap of red black binary search tress // should I use another DS?

    std::vector<std::string> stockList = {"AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "FB", "NFLX", "NVDA", "BABA", "INTC", "CSCO", "ORCL", "IBM", "ADBE", "CRM", "PYPL", "QCOM", "AMD", "TXN", "AVGO"};
    for(const auto& stock : stockList){
        orders[stock].sellOrders = std::set<SellOrder>();
        orders[stock].buyOrders = std::set<BuyOrder>();
    }

    // TODO: remove
    populate_exchange_with_test_data(&orders, &stockList);
    for(auto& stock : stockList){
        match_orders(&orders, &stock);
    }

    // create broadcast socket
    int broadcastfd;
    if ((broadcastfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int broadcast_opt = 1;
    if(setsockopt(broadcastfd, SOL_SOCKET, SO_BROADCAST, &broadcast_opt, sizeof broadcast_opt) == -1){
        perror("setsockopt failed");
        exit(EXIT_FAILURE); 
    }

    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof broadcast_addr);
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(BROADCAST_PORT);
    broadcast_addr.sin_addr.s_addr = inet_addr(BROADCAST_IP);

    // create listening socket
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd == -1){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    int listen_opt = 1;
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &listen_opt, sizeof(listen_opt)) == -1) {
        perror("setsockopt failed");
        return EXIT_FAILURE;
    }

    sockaddr_in myAddr;
    memset(&myAddr, 0, sizeof myAddr);
    myAddr.sin_family = AF_INET;
    myAddr.sin_addr.s_addr = INADDR_ANY;
    myAddr.sin_port = htons(LISTEN_PORT);

    if(bind(listenfd, (struct sockaddr*)&myAddr, sizeof myAddr) == -1){
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(listenfd, 64) == -1) {
        perror("listen failed");
        return EXIT_FAILURE;
    }

    if (fcntl(listenfd, F_SETFL, fcntl(listenfd, F_GETFL, 0) | O_NONBLOCK) == -1){ // set nonblocking
        perror("fcntl failed");
    }

    // init epoll
    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        perror("epoll_create1 failed");
        return EXIT_FAILURE;
    }

    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = listenfd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, listenfd, &event) == -1) {
        perror("epoll_ctl failed");
        return EXIT_FAILURE;
    }
    struct epoll_event events[MAXEVENTS];
    int nfds = 0;

    // OPTION: can optimize by using edge-triggered polling and adding open tcp connections to events (increase max_events to servers + 1)
    while(true){
        // event loop
        // handle incoming requests
        nfds = epoll_wait(epollfd, events, MAXEVENTS, BROADCAST_TIMEOUT_MS);
        if (nfds == -1) {
            perror("epoll_wait failed");
            exit(EXIT_FAILURE);
        }
        // handle matching requests
        if(events[0].events & EPOLLIN && !(events[0].events & EPOLLERR)){
            struct sockaddr_in client_addr;
            socklen_t client_addr_len = sizeof(client_addr);
            int client_fd = accept(listenfd, (struct sockaddr *)&client_addr, &client_addr_len);
            if(client_fd == -1){
                perror("accept failed");
            }

            // TODO: AUTH, request validation
            capnp::StreamFdMessageReader messageReader(client_fd);
            auto request = messageReader.getRoot<NetworkRequest>();
            std::pair<bool, std::string> rsp;
            if(request.isMakeOrderRequest()){
                rsp = handleMakeOrderRequest(&request, &orders);
            } else if(request.isCancelOrderRequest()){
                rsp = handleCancelOrderRequest(&request, &orders);
            } else {
                perror("unknown request type");
            }

            // perform matching
            if(rsp.first){
                match_orders(&orders, &(rsp.second));
            }
            // TODO: provide a response

            close(client_fd);
        }
        // broadcast_opt requests to buy/sell
        broadcast_market_data(broadcastfd, &broadcast_addr, &orders);
    }
    return 0;
}