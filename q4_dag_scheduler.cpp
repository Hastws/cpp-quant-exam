/**
 * 题目四：多DAG管理与执行调度 (MultiDAGScheduler)
 * 支持管理多个独立的DAG，按规则完成拓扑排序与执行顺序规划。
 * 编译: g++ -std=c++11 -o q4_dag_scheduler q4_dag_scheduler.cpp
 */

#include <iostream>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <queue>
#include <algorithm>
#include <cassert>

// 单个节点
struct Node {
    uint64_t id;
    Node(uint64_t node_id) : id(node_id) {}
};

// 单个DAG的元信息
struct DAGMeta {
    std::string dag_name;
    uint64_t dag_id;
    DAGMeta(uint64_t id, const std::string& name) : dag_id(id), dag_name(name) {}
};

class MultiDAGScheduler {
private:
    // 每个DAG内部的结构
    struct DAGData {
        DAGMeta meta;
        std::unordered_set<uint64_t> node_ids;
        std::unordered_map<uint64_t, std::vector<uint64_t>> adj;   // from -> [to...]
        std::unordered_map<uint64_t, uint64_t> in_degree;
        // 边集合，用于去重
        std::unordered_map<uint64_t, std::unordered_set<uint64_t>> edge_set;

        DAGData(const DAGMeta& m) : meta(m) {}
    };

    std::unordered_map<uint64_t, DAGData> dags_;

    // DAG间的依赖关系
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> dag_children_;    // parent -> {children}
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> dag_parents_;     // child -> {parents}

    // 检测DAG间是否存在环
    bool has_dag_cycle() const {
        std::unordered_map<uint64_t, uint64_t> in_deg;
        for (const auto& kv : dags_) {
            in_deg[kv.first] = 0;
        }
        for (const auto& kv : dag_children_) {
            for (uint64_t child : kv.second) {
                in_deg[child]++;
            }
        }

        std::queue<uint64_t> q;
        for (const auto& kv : in_deg) {
            if (kv.second == 0) {
                q.push(kv.first);
            }
        }

        uint64_t count = 0;
        while (!q.empty()) {
            uint64_t dag_id = q.front();
            q.pop();
            count++;
            auto it = dag_children_.find(dag_id);
            if (it != dag_children_.end()) {
                for (uint64_t child : it->second) {
                    in_deg[child]--;
                    if (in_deg[child] == 0) {
                        q.push(child);
                    }
                }
            }
        }
        return count != dags_.size();
    }

public:
    // ==================== 第一部分：基础单DAG能力 ====================

    // 添加一个新的空DAG
    bool add_dag(const DAGMeta& dag_meta) {
        if (dags_.find(dag_meta.dag_id) != dags_.end()) {
            return false; // ID重复
        }
        dags_.emplace(dag_meta.dag_id, DAGData(dag_meta));
        return true;
    }

    // 向指定DAG添加节点
    bool add_node_to_dag(uint64_t dag_id, const Node& node) {
        auto it = dags_.find(dag_id);
        if (it == dags_.end()) return false; // DAG不存在

        DAGData& dag = it->second;
        if (dag.node_ids.count(node.id)) return false; // 节点ID重复

        dag.node_ids.insert(node.id);
        dag.in_degree[node.id] = 0;
        return true;
    }

    // 向指定DAG添加有向边
    bool add_edge_to_dag(uint64_t dag_id, uint64_t from_node_id, uint64_t to_node_id) {
        auto it = dags_.find(dag_id);
        if (it == dags_.end()) return false; // DAG不存在

        DAGData& dag = it->second;
        if (!dag.node_ids.count(from_node_id) || !dag.node_ids.count(to_node_id)) {
            return false; // 节点不存在
        }

        // 检查重复边
        if (dag.edge_set[from_node_id].count(to_node_id)) {
            return false;
        }

        dag.adj[from_node_id].push_back(to_node_id);
        dag.in_degree[to_node_id]++;
        dag.edge_set[from_node_id].insert(to_node_id);
        return true;
    }

    // 校验指定DAG是否为合法无环图
    bool is_dag_valid(uint64_t dag_id) const {
        auto it = dags_.find(dag_id);
        if (it == dags_.end()) return false;

        const DAGData& dag = it->second;
        if (dag.node_ids.empty()) return true; // 空DAG合法

        // 使用 Kahn 算法检测环
        std::unordered_map<uint64_t, uint64_t> in_deg = dag.in_degree;
        std::queue<uint64_t> q;

        for (const auto& kv : in_deg) {
            if (kv.second == 0) {
                q.push(kv.first);
            }
        }

        uint64_t count = 0;
        while (!q.empty()) {
            uint64_t node = q.front();
            q.pop();
            count++;
            auto adj_it = dag.adj.find(node);
            if (adj_it != dag.adj.end()) {
                for (uint64_t next : adj_it->second) {
                    in_deg[next]--;
                    if (in_deg[next] == 0) {
                        q.push(next);
                    }
                }
            }
        }

        return count == dag.node_ids.size();
    }

    // 返回单个DAG的拓扑排序
    std::vector<uint64_t> get_dag_topological_order(uint64_t dag_id) const {
        if (!is_dag_valid(dag_id)) return {};

        auto it = dags_.find(dag_id);
        const DAGData& dag = it->second;

        std::unordered_map<uint64_t, uint64_t> in_deg = dag.in_degree;
        std::queue<uint64_t> q;

        for (const auto& kv : in_deg) {
            if (kv.second == 0) {
                q.push(kv.first);
            }
        }

        std::vector<uint64_t> result;
        while (!q.empty()) {
            uint64_t node = q.front();
            q.pop();
            result.push_back(node);
            auto adj_it = dag.adj.find(node);
            if (adj_it != dag.adj.end()) {
                for (uint64_t next : adj_it->second) {
                    in_deg[next]--;
                    if (in_deg[next] == 0) {
                        q.push(next);
                    }
                }
            }
        }

        return result;
    }

    // ==================== 第二部分：多DAG调度核心 ====================

    // 设置DAG间的依赖关系
    bool set_dag_dependency(uint64_t parent_dag_id, uint64_t child_dag_id) {
        if (dags_.find(parent_dag_id) == dags_.end() ||
            dags_.find(child_dag_id) == dags_.end()) {
            return false; // DAG不存在
        }

        // 先临时添加依赖，检测是否形成环
        dag_children_[parent_dag_id].insert(child_dag_id);
        dag_parents_[child_dag_id].insert(parent_dag_id);

        if (has_dag_cycle()) {
            // 回滚
            dag_children_[parent_dag_id].erase(child_dag_id);
            dag_parents_[child_dag_id].erase(parent_dag_id);
            return false;
        }

        return true;
    }

    // 生成全局执行计划
    std::vector<std::vector<uint64_t>> get_global_execution_plan() const {
        // 检查所有DAG是否合法
        for (const auto& kv : dags_) {
            if (!is_dag_valid(kv.first)) {
                return {};
            }
        }

        // 检查DAG间是否有循环依赖
        if (has_dag_cycle()) {
            return {};
        }

        // 计算每个DAG在DAG间依赖图中的入度
        std::unordered_map<uint64_t, uint64_t> in_deg;
        for (const auto& kv : dags_) {
            in_deg[kv.first] = 0;
        }
        for (const auto& kv : dag_children_) {
            for (uint64_t child : kv.second) {
                in_deg[child]++;
            }
        }

        // BFS 层级遍历，同一层的DAG可并行
        std::vector<std::vector<uint64_t>> plan;
        std::queue<uint64_t> q;

        for (const auto& kv : in_deg) {
            if (kv.second == 0) {
                q.push(kv.first);
            }
        }

        while (!q.empty()) {
            std::vector<uint64_t> batch;
            int size = q.size();
            for (int i = 0; i < size; ++i) {
                uint64_t dag_id = q.front();
                q.pop();
                batch.push_back(dag_id);
            }
            // 排序使输出稳定
            std::sort(batch.begin(), batch.end());
            plan.push_back(batch);

            for (uint64_t dag_id : batch) {
                auto it = dag_children_.find(dag_id);
                if (it != dag_children_.end()) {
                    for (uint64_t child : it->second) {
                        in_deg[child]--;
                        if (in_deg[child] == 0) {
                            q.push(child);
                        }
                    }
                }
            }
        }

        return plan;
    }
};

// ===================== 辅助打印 =====================

void print_topo(const std::string& label, const std::vector<uint64_t>& order) {
    std::cout << "  " << label << ": [";
    for (size_t i = 0; i < order.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << order[i];
    }
    std::cout << "]" << std::endl;
}

void print_plan(const std::vector<std::vector<uint64_t>>& plan) {
    std::cout << "  执行计划: [";
    for (size_t i = 0; i < plan.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << "[";
        for (size_t j = 0; j < plan[i].size(); ++j) {
            if (j > 0) std::cout << ",";
            std::cout << plan[i][j];
        }
        std::cout << "]";
    }
    std::cout << "]" << std::endl;
}

// ===================== 测试 =====================

int main() {
    // 测试1: 基础DAG操作 - 添加DAG、节点、边、拓扑排序
    {
        std::cout << "=== 测试1: 基础DAG操作 ===" << std::endl;
        MultiDAGScheduler scheduler;

        assert(scheduler.add_dag(DAGMeta(1, "Factor_A")));
        assert(!scheduler.add_dag(DAGMeta(1, "Duplicate"))); // ID重复

        assert(scheduler.add_node_to_dag(1, Node(10)));
        assert(scheduler.add_node_to_dag(1, Node(20)));
        assert(scheduler.add_node_to_dag(1, Node(30)));
        assert(!scheduler.add_node_to_dag(1, Node(10))); // 节点ID重复
        assert(!scheduler.add_node_to_dag(99, Node(40))); // DAG不存在

        assert(scheduler.add_edge_to_dag(1, 10, 20));
        assert(scheduler.add_edge_to_dag(1, 20, 30));
        assert(!scheduler.add_edge_to_dag(1, 10, 20)); // 重复边
        assert(!scheduler.add_edge_to_dag(1, 10, 99)); // 节点不存在
        assert(!scheduler.add_edge_to_dag(99, 10, 20)); // DAG不存在

        assert(scheduler.is_dag_valid(1));
        assert(!scheduler.is_dag_valid(99)); // 不存在

        auto order = scheduler.get_dag_topological_order(1);
        print_topo("DAG 1 拓扑序", order);
        assert(order.size() == 3);
        // 拓扑序: 10 -> 20 -> 30
        // 验证拓扑约束: 10在20前, 20在30前
        auto pos10 = std::find(order.begin(), order.end(), 10) - order.begin();
        auto pos20 = std::find(order.begin(), order.end(), 20) - order.begin();
        auto pos30 = std::find(order.begin(), order.end(), 30) - order.begin();
        assert(pos10 < pos20);
        assert(pos20 < pos30);

        std::cout << "[PASS] 基础DAG操作" << std::endl << std::endl;
    }

    // 测试2: DAG内部有环
    {
        std::cout << "=== 测试2: DAG内部有环 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "Cyclic"));
        scheduler.add_node_to_dag(1, Node(1));
        scheduler.add_node_to_dag(1, Node(2));
        scheduler.add_node_to_dag(1, Node(3));
        scheduler.add_edge_to_dag(1, 1, 2);
        scheduler.add_edge_to_dag(1, 2, 3);
        scheduler.add_edge_to_dag(1, 3, 1); // 形成环

        assert(!scheduler.is_dag_valid(1));
        assert(scheduler.get_dag_topological_order(1).empty());

        std::cout << "[PASS] DAG内部有环检测" << std::endl << std::endl;
    }

    // 测试3: 场景1 - 无DAG间依赖，所有DAG同一批次
    {
        std::cout << "=== 测试3: 无DAG间依赖 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "DAG_1"));
        scheduler.add_dag(DAGMeta(2, "DAG_2"));
        scheduler.add_dag(DAGMeta(3, "DAG_3"));

        // 给每个DAG加点节点使其不为空
        scheduler.add_node_to_dag(1, Node(10));
        scheduler.add_node_to_dag(2, Node(20));
        scheduler.add_node_to_dag(3, Node(30));

        auto plan = scheduler.get_global_execution_plan();
        print_plan(plan);

        assert(plan.size() == 1);
        assert(plan[0].size() == 3);
        // 排序后应为 [1, 2, 3]
        assert(plan[0][0] == 1);
        assert(plan[0][1] == 2);
        assert(plan[0][2] == 3);

        std::cout << "[PASS] 无DAG间依赖" << std::endl << std::endl;
    }

    // 测试4: 场景2 - 有DAG间依赖
    // 设置依赖：1→2、1→3、3→4
    // 期望: [[1], [2,3], [4]]
    {
        std::cout << "=== 测试4: 有DAG间依赖 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "DAG_1"));
        scheduler.add_dag(DAGMeta(2, "DAG_2"));
        scheduler.add_dag(DAGMeta(3, "DAG_3"));
        scheduler.add_dag(DAGMeta(4, "DAG_4"));

        scheduler.add_node_to_dag(1, Node(10));
        scheduler.add_node_to_dag(2, Node(20));
        scheduler.add_node_to_dag(3, Node(30));
        scheduler.add_node_to_dag(4, Node(40));

        assert(scheduler.set_dag_dependency(1, 2));
        assert(scheduler.set_dag_dependency(1, 3));
        assert(scheduler.set_dag_dependency(3, 4));

        auto plan = scheduler.get_global_execution_plan();
        print_plan(plan);

        assert(plan.size() == 3);
        assert(plan[0] == std::vector<uint64_t>{1});
        assert(plan[1] == (std::vector<uint64_t>{2, 3}));
        assert(plan[2] == std::vector<uint64_t>{4});

        std::cout << "[PASS] 有DAG间依赖" << std::endl << std::endl;
    }

    // 测试5: 场景3 - DAG间循环依赖
    {
        std::cout << "=== 测试5: DAG间循环依赖 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "DAG_1"));
        scheduler.add_dag(DAGMeta(2, "DAG_2"));

        scheduler.add_node_to_dag(1, Node(10));
        scheduler.add_node_to_dag(2, Node(20));

        assert(scheduler.set_dag_dependency(1, 2));
        assert(!scheduler.set_dag_dependency(2, 1)); // 应该返回false，形成环

        // 由于set_dag_dependency回滚了环，执行计划应仍合法
        auto plan = scheduler.get_global_execution_plan();
        print_plan(plan);
        assert(plan.size() == 2);

        std::cout << "[PASS] DAG间循环依赖检测" << std::endl << std::endl;
    }

    // 测试6: 单个DAG内部有环导致全局执行计划返回空
    {
        std::cout << "=== 测试6: 内部有环导致全局计划为空 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "Valid_DAG"));
        scheduler.add_dag(DAGMeta(2, "Cyclic_DAG"));

        scheduler.add_node_to_dag(1, Node(10));
        scheduler.add_node_to_dag(1, Node(20));
        scheduler.add_edge_to_dag(1, 10, 20);

        scheduler.add_node_to_dag(2, Node(30));
        scheduler.add_node_to_dag(2, Node(40));
        scheduler.add_edge_to_dag(2, 30, 40);
        scheduler.add_edge_to_dag(2, 40, 30); // DAG 2 有环

        auto plan = scheduler.get_global_execution_plan();
        assert(plan.empty());
        std::cout << "  执行计划: (empty)" << std::endl;

        std::cout << "[PASS] 内部有环导致全局计划为空" << std::endl << std::endl;
    }

    // 测试7: 空DAG也合法
    {
        std::cout << "=== 测试7: 空DAG合法 ===" << std::endl;
        MultiDAGScheduler scheduler;
        scheduler.add_dag(DAGMeta(1, "Empty_DAG"));

        assert(scheduler.is_dag_valid(1));
        auto order = scheduler.get_dag_topological_order(1);
        assert(order.empty());

        auto plan = scheduler.get_global_execution_plan();
        assert(plan.size() == 1);
        assert(plan[0] == std::vector<uint64_t>{1});

        std::cout << "[PASS] 空DAG合法" << std::endl << std::endl;
    }

    // 测试8: 复杂DAG间依赖（菱形依赖）
    // 1→2, 1→3, 2→4, 3→4
    // 期望: [[1], [2,3], [4]]
    {
        std::cout << "=== 测试8: 菱形DAG依赖 ===" << std::endl;
        MultiDAGScheduler scheduler;
        for (uint64_t i = 1; i <= 4; ++i) {
            scheduler.add_dag(DAGMeta(i, "DAG_" + std::to_string(i)));
            scheduler.add_node_to_dag(i, Node(i * 10));
        }

        assert(scheduler.set_dag_dependency(1, 2));
        assert(scheduler.set_dag_dependency(1, 3));
        assert(scheduler.set_dag_dependency(2, 4));
        assert(scheduler.set_dag_dependency(3, 4));

        auto plan = scheduler.get_global_execution_plan();
        print_plan(plan);

        assert(plan.size() == 3);
        assert(plan[0] == std::vector<uint64_t>{1});
        assert(plan[1] == (std::vector<uint64_t>{2, 3}));
        assert(plan[2] == std::vector<uint64_t>{4});

        std::cout << "[PASS] 菱形DAG依赖" << std::endl << std::endl;
    }

    std::cout << "所有测试通过！" << std::endl;
    return 0;
}
