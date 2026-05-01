/**
 * 题目二：单例模式 - 线程安全的懒汉式 ConfigManager
 * 量化回测系统配置管理器，全局唯一实例，管理回测参数。
 * 编译: g++ -std=c++11 -pthread -o q2_singleton q2_singleton.cpp
 */

#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <vector>
#include <cassert>

class ConfigManager {
public:
    // 获取单例实例（懒汉式，线程安全）
    static ConfigManager& getInstance() {
        // C++11 保证局部静态变量初始化的线程安全性（Magic Static）
        static ConfigManager instance;
        return instance;
    }

    // 禁止拷贝和移动
    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;
    ConfigManager(ConfigManager&&) = delete;
    ConfigManager& operator=(ConfigManager&&) = delete;

    // 设置参数
    void set(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        config_[key] = value;
    }

    // 获取参数
    std::string get(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = config_.find(key);
        if (it != config_.end()) {
            return it->second;
        }
        return "";
    }

    // 判断是否存在某个参数
    bool has(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return config_.find(key) != config_.end();
    }

    // 获取初始资金
    double getInitialCapital() const {
        std::string val = get("initial_capital");
        return val.empty() ? 0.0 : std::stod(val);
    }

    // 获取手续费率
    double getCommissionRate() const {
        std::string val = get("commission_rate");
        return val.empty() ? 0.0 : std::stod(val);
    }

    // 获取滑点
    double getSlippage() const {
        std::string val = get("slippage");
        return val.empty() ? 0.0 : std::stod(val);
    }

    // 打印所有配置
    void printAll() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "===== 当前配置 =====" << std::endl;
        for (const auto& kv : config_) {
            std::cout << "  " << kv.first << " = " << kv.second << std::endl;
        }
        std::cout << "====================" << std::endl;
    }

private:
    // 私有构造函数
    ConfigManager() {
        std::cout << "[ConfigManager] 实例已创建" << std::endl;
    }

    ~ConfigManager() {
        std::cout << "[ConfigManager] 实例已销毁" << std::endl;
    }

    std::unordered_map<std::string, std::string> config_;
    mutable std::mutex mutex_;
};

// ===================== 测试 =====================

int main() {
    // 测试1: 单例唯一性
    {
        ConfigManager& cm1 = ConfigManager::getInstance();
        ConfigManager& cm2 = ConfigManager::getInstance();
        assert(&cm1 == &cm2);
        std::cout << "[PASS] 单例唯一性验证" << std::endl;
    }

    // 测试2: 基本配置存取
    {
        ConfigManager& cm = ConfigManager::getInstance();
        cm.set("initial_capital", "1000000");
        cm.set("commission_rate", "0.0003");
        cm.set("slippage", "0.01");

        assert(cm.get("initial_capital") == "1000000");
        assert(cm.get("commission_rate") == "0.0003");
        assert(cm.get("slippage") == "0.01");
        assert(cm.get("nonexistent") == "");

        assert(cm.has("initial_capital"));
        assert(!cm.has("nonexistent"));

        assert(cm.getInitialCapital() == 1000000.0);
        assert(cm.getCommissionRate() == 0.0003);
        assert(cm.getSlippage() == 0.01);

        cm.printAll();
        std::cout << "[PASS] 基本配置存取" << std::endl;
    }

    // 测试3: 多线程安全性
    {
        ConfigManager& cm = ConfigManager::getInstance();
        const int num_threads = 10;
        const int ops_per_thread = 1000;
        std::vector<std::thread> threads;

        // 多线程并发写入
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&cm, i, ops_per_thread]() {
                for (int j = 0; j < ops_per_thread; ++j) {
                    std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                    cm.set(key, std::to_string(i * ops_per_thread + j));
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // 验证: 所有写入的值都能正确读取
        bool all_valid = true;
        for (int i = 0; i < num_threads; ++i) {
            for (int j = 0; j < ops_per_thread; ++j) {
                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                if (!cm.has(key)) {
                    all_valid = false;
                    break;
                }
            }
            if (!all_valid) break;
        }
        assert(all_valid);
        std::cout << "[PASS] 多线程安全性 (" << num_threads << "线程 x "
                  << ops_per_thread << "次操作)" << std::endl;
    }

    // 测试4: 多线程同时获取实例
    {
        std::vector<std::thread> threads;
        std::vector<ConfigManager*> ptrs(8, nullptr);

        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([&ptrs, i]() {
                ptrs[i] = &ConfigManager::getInstance();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // 所有线程获取的都是同一个实例
        for (int i = 1; i < 8; ++i) {
            assert(ptrs[i] == ptrs[0]);
        }
        std::cout << "[PASS] 多线程获取实例唯一性" << std::endl;
    }

    std::cout << "\n所有测试通过！" << std::endl;
    return 0;
}
