/**
 * 题目三：限价订单簿撮合引擎 (OrderBookMatcher)
 * 遵循"价格优先、时间优先"原则完成订单撮合。
 * 编译: g++ -std=c++11 -o q3_order_book q3_order_book.cpp
 */

#include <iostream>
#include <cstdint>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <cassert>
#include <cmath>

// 订单类型
enum class OrderType {
    BUY,  // 买单（以 ≤ 委托价买入）
    SELL  // 卖单（以 ≥ 委托价卖出）
};

// 订单结构体
struct Order {
    uint64_t order_id;      // 唯一订单 ID
    OrderType type;         // 订单类型
    double price;           // 委托价格
    uint64_t quantity;      // 委托总数量
    uint64_t timestamp;     // 委托时间戳（毫秒级）
    uint64_t remain_qty;    // 剩余未成交数量

    Order(uint64_t id, OrderType t, double p, uint64_t q, uint64_t ts)
        : order_id(id), type(t), price(p), quantity(q), timestamp(ts), remain_qty(q) {}
};

// 成交记录
struct Trade {
    uint64_t trade_id;      // 自增成交 ID
    uint64_t buy_order_id;  // 买单ID
    uint64_t sell_order_id; // 卖单ID
    double price;           // 成交价格（以对手方价格为准）
    uint64_t quantity;      // 成交数量
    uint64_t timestamp;     // 成交时间戳

    Trade(uint64_t tid, uint64_t bid, uint64_t sid, double p, uint64_t q, uint64_t ts)
        : trade_id(tid), buy_order_id(bid), sell_order_id(sid), price(p), quantity(q), timestamp(ts) {}
};

class OrderBookMatcher {
private:
    // 买单排序：价格从高到低（价格优先），相同价格按时间从小到大（时间优先）
    struct BuyCompare {
        bool operator()(const Order& a, const Order& b) const {
            if (a.price != b.price) return a.price > b.price;       // 高价优先
            if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp; // 早时间优先
            return a.order_id < b.order_id;
        }
    };

    // 卖单排序：价格从低到高（价格优先），相同价格按时间从小到大（时间优先）
    struct SellCompare {
        bool operator()(const Order& a, const Order& b) const {
            if (a.price != b.price) return a.price < b.price;       // 低价优先
            if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp; // 早时间优先
            return a.order_id < b.order_id;
        }
    };

    std::set<Order, BuyCompare> buy_book_;   // 买单簿
    std::set<Order, SellCompare> sell_book_;  // 卖单簿
    uint64_t next_trade_id_;

public:
    OrderBookMatcher() : next_trade_id_(1) {}

    // 添加订单
    bool add_order(const Order& order) {
        if (order.remain_qty == 0) return false;

        if (order.type == OrderType::BUY) {
            buy_book_.insert(order);
        } else {
            sell_book_.insert(order);
        }
        return true;
    }

    // 执行撮合逻辑
    std::vector<Trade> match() {
        std::vector<Trade> trades;

        while (!buy_book_.empty() && !sell_book_.empty()) {
            // 取买单簿最优买单（最高价）和卖单簿最优卖单（最低价）
            auto buy_it = buy_book_.begin();
            auto sell_it = sell_book_.begin();

            // 如果最高买价 < 最低卖价，则无法撮合
            if (buy_it->price < sell_it->price) {
                break;
            }

            // 拷贝出来修改（set 中元素不能直接修改）
            Order buy_order = *buy_it;
            Order sell_order = *sell_it;
            buy_book_.erase(buy_it);
            sell_book_.erase(sell_it);

            // 成交数量为两者剩余数量的较小值
            uint64_t match_qty = std::min(buy_order.remain_qty, sell_order.remain_qty);

            // 成交价格以对手方委托价为准
            // 买单匹配卖单时，成交价为卖单价格（对手方价格）
            double trade_price = sell_order.price;

            // 成交时间戳取较大值
            uint64_t trade_ts = std::max(buy_order.timestamp, sell_order.timestamp);

            Trade trade(next_trade_id_++, buy_order.order_id, sell_order.order_id,
                        trade_price, match_qty, trade_ts);
            trades.push_back(trade);

            // 更新剩余数量
            buy_order.remain_qty -= match_qty;
            sell_order.remain_qty -= match_qty;

            // 部分成交的订单放回订单簿
            if (buy_order.remain_qty > 0) {
                buy_book_.insert(buy_order);
            }
            if (sell_order.remain_qty > 0) {
                sell_book_.insert(sell_order);
            }
        }

        return trades;
    }

    // 获取当前买单簿（价格优先、时间优先）
    std::vector<Order> get_buy_book() const {
        return std::vector<Order>(buy_book_.begin(), buy_book_.end());
    }

    // 获取当前卖单簿（价格优先、时间优先）
    std::vector<Order> get_sell_book() const {
        return std::vector<Order>(sell_book_.begin(), sell_book_.end());
    }
};

// ===================== 辅助打印函数 =====================

void print_trades(const std::vector<Trade>& trades) {
    for (const auto& t : trades) {
        std::cout << "  Trade#" << t.trade_id
                  << " Buy:" << t.buy_order_id
                  << " Sell:" << t.sell_order_id
                  << " Price:" << t.price
                  << " Qty:" << t.quantity
                  << " TS:" << t.timestamp << std::endl;
    }
}

void print_book(const std::string& label, const std::vector<Order>& book) {
    std::cout << "  " << label << ": ";
    if (book.empty()) {
        std::cout << "(empty)";
    }
    for (const auto& o : book) {
        std::cout << "[ID:" << o.order_id << " P:" << o.price
                  << " Remain:" << o.remain_qty << " TS:" << o.timestamp << "] ";
    }
    std::cout << std::endl;
}

// ===================== 测试 =====================

int main() {
    // 测试1: 买单与卖单完全成交
    {
        std::cout << "=== 测试1: 完全成交 ===" << std::endl;
        OrderBookMatcher matcher;
        matcher.add_order(Order(1, OrderType::BUY, 10.0, 100, 1000));
        matcher.add_order(Order(2, OrderType::SELL, 9.9, 100, 1001));

        auto trades = matcher.match();
        print_trades(trades);

        assert(trades.size() == 1);
        assert(trades[0].buy_order_id == 1);
        assert(trades[0].sell_order_id == 2);
        assert(std::abs(trades[0].price - 9.9) < 1e-9);  // 以卖方价格成交
        assert(trades[0].quantity == 100);

        assert(matcher.get_buy_book().empty());
        assert(matcher.get_sell_book().empty());
        std::cout << "[PASS] 完全成交" << std::endl << std::endl;
    }

    // 测试2: 部分成交（买单剩余留在订单簿）
    {
        std::cout << "=== 测试2: 部分成交 ===" << std::endl;
        OrderBookMatcher matcher;
        matcher.add_order(Order(1, OrderType::BUY, 10.0, 200, 1000));
        matcher.add_order(Order(2, OrderType::SELL, 9.8, 80, 1001));

        auto trades = matcher.match();
        print_trades(trades);

        assert(trades.size() == 1);
        assert(trades[0].quantity == 80);
        assert(std::abs(trades[0].price - 9.8) < 1e-9);

        auto buy_book = matcher.get_buy_book();
        assert(buy_book.size() == 1);
        assert(buy_book[0].remain_qty == 120);
        assert(matcher.get_sell_book().empty());

        print_book("买单簿", buy_book);
        std::cout << "[PASS] 部分成交" << std::endl << std::endl;
    }

    // 测试3: 多笔同价格订单按时间优先撮合
    {
        std::cout << "=== 测试3: 同价格时间优先 ===" << std::endl;
        OrderBookMatcher matcher;
        // 三个卖单，同价格不同时间
        matcher.add_order(Order(10, OrderType::SELL, 10.0, 50, 2000));
        matcher.add_order(Order(11, OrderType::SELL, 10.0, 50, 1000));  // 更早
        matcher.add_order(Order(12, OrderType::SELL, 10.0, 50, 3000));

        // 一个买单，数量只够匹配两笔
        matcher.add_order(Order(20, OrderType::BUY, 10.0, 80, 4000));

        auto trades = matcher.match();
        print_trades(trades);

        // 应先匹配 ID=11(ts=1000)，再匹配 ID=10(ts=2000)
        assert(trades.size() == 2);
        assert(trades[0].sell_order_id == 11);
        assert(trades[0].quantity == 50);
        assert(trades[1].sell_order_id == 10);
        assert(trades[1].quantity == 30);

        auto sell_book = matcher.get_sell_book();
        print_book("卖单簿", sell_book);

        // 卖单簿剩余: ID=10(remain=20), ID=12(remain=50)
        assert(sell_book.size() == 2);
        assert(sell_book[0].order_id == 10);
        assert(sell_book[0].remain_qty == 20);
        assert(sell_book[1].order_id == 12);
        assert(sell_book[1].remain_qty == 50);

        assert(matcher.get_buy_book().empty());
        std::cout << "[PASS] 同价格时间优先" << std::endl << std::endl;
    }

    // 测试4: 无匹配对手单（订单直接进入订单簿）
    {
        std::cout << "=== 测试4: 无匹配对手单 ===" << std::endl;
        OrderBookMatcher matcher;
        matcher.add_order(Order(1, OrderType::BUY, 9.5, 100, 1000));
        matcher.add_order(Order(2, OrderType::SELL, 10.0, 100, 1001));

        auto trades = matcher.match();
        assert(trades.empty());

        auto buy_book = matcher.get_buy_book();
        auto sell_book = matcher.get_sell_book();
        assert(buy_book.size() == 1);
        assert(sell_book.size() == 1);

        print_book("买单簿", buy_book);
        print_book("卖单簿", sell_book);
        std::cout << "[PASS] 无匹配对手单" << std::endl << std::endl;
    }

    // 测试5: 价格优先 - 多个不同价格的卖单
    {
        std::cout << "=== 测试5: 价格优先 ===" << std::endl;
        OrderBookMatcher matcher;
        matcher.add_order(Order(1, OrderType::SELL, 10.5, 100, 1000));
        matcher.add_order(Order(2, OrderType::SELL, 9.8, 100, 1001));
        matcher.add_order(Order(3, OrderType::SELL, 10.2, 100, 1002));

        // 买单价格足够高，应先匹配最低价的卖单
        matcher.add_order(Order(10, OrderType::BUY, 10.5, 150, 2000));

        auto trades = matcher.match();
        print_trades(trades);

        // 先匹配 ID=2(9.8), 成交100; 再匹配 ID=3(10.2), 成交50
        assert(trades.size() == 2);
        assert(trades[0].sell_order_id == 2);
        assert(std::abs(trades[0].price - 9.8) < 1e-9);
        assert(trades[0].quantity == 100);
        assert(trades[1].sell_order_id == 3);
        assert(std::abs(trades[1].price - 10.2) < 1e-9);
        assert(trades[1].quantity == 50);

        auto sell_book = matcher.get_sell_book();
        print_book("卖单簿", sell_book);
        // 卖单簿剩余: ID=3(remain=50), ID=1(remain=100)
        assert(sell_book.size() == 2);

        std::cout << "[PASS] 价格优先" << std::endl << std::endl;
    }

    // 测试6: 连续撮合
    {
        std::cout << "=== 测试6: 连续撮合 ===" << std::endl;
        OrderBookMatcher matcher;
        matcher.add_order(Order(1, OrderType::SELL, 10.0, 100, 1000));
        auto t1 = matcher.match();
        assert(t1.empty()); // 无对手

        matcher.add_order(Order(2, OrderType::BUY, 10.0, 50, 2000));
        auto t2 = matcher.match();
        assert(t2.size() == 1);
        assert(t2[0].quantity == 50);

        matcher.add_order(Order(3, OrderType::BUY, 10.0, 30, 3000));
        auto t3 = matcher.match();
        assert(t3.size() == 1);
        assert(t3[0].quantity == 30);

        auto sell_book = matcher.get_sell_book();
        assert(sell_book.size() == 1);
        assert(sell_book[0].remain_qty == 20);

        print_book("卖单簿", sell_book);
        std::cout << "[PASS] 连续撮合" << std::endl << std::endl;
    }

    std::cout << "所有测试通过！" << std::endl;
    return 0;
}
