/**
 * 题目一：手写共享指针 (SharedPtr)
 * 模拟 std::shared_ptr 的核心逻辑，重点考察引用计数、内存管理和拷贝/移动语义。
 * 编译: g++ -std=c++11 -o q1_shared_ptr q1_shared_ptr.cpp
 */

#include <iostream>
#include <cassert>
#include <cstddef>
#include <utility>

template <typename T>
class SharedPtr {
private:
    T* ptr_;
    std::size_t* ref_count_;

    void release() {
        if (ref_count_) {
            --(*ref_count_);
            if (*ref_count_ == 0) {
                delete ptr_;
                delete ref_count_;
            }
            ptr_ = nullptr;
            ref_count_ = nullptr;
        }
    }

public:
    // 默认构造
    SharedPtr() : ptr_(nullptr), ref_count_(nullptr) {}

    // 接收原始指针的构造函数
    explicit SharedPtr(T* raw) : ptr_(raw), ref_count_(nullptr) {
        if (ptr_) {
            ref_count_ = new std::size_t(1);
        }
    }

    // 析构函数
    ~SharedPtr() {
        release();
    }

    // 拷贝构造（引用计数+1）
    SharedPtr(const SharedPtr& other) : ptr_(other.ptr_), ref_count_(other.ref_count_) {
        if (ref_count_) {
            ++(*ref_count_);
        }
    }

    // 拷贝赋值运算符（引用计数+1）
    SharedPtr& operator=(const SharedPtr& other) {
        if (this != &other) {
            release();
            ptr_ = other.ptr_;
            ref_count_ = other.ref_count_;
            if (ref_count_) {
                ++(*ref_count_);
            }
        }
        return *this;
    }

    // 移动构造（转移所有权，不增加引用计数）
    SharedPtr(SharedPtr&& other) noexcept : ptr_(other.ptr_), ref_count_(other.ref_count_) {
        other.ptr_ = nullptr;
        other.ref_count_ = nullptr;
    }

    // 移动赋值运算符（转移所有权）
    SharedPtr& operator=(SharedPtr&& other) noexcept {
        if (this != &other) {
            release();
            ptr_ = other.ptr_;
            ref_count_ = other.ref_count_;
            other.ptr_ = nullptr;
            other.ref_count_ = nullptr;
        }
        return *this;
    }

    // 重载 * 运算符
    T& operator*() const {
        return *ptr_;
    }

    // 重载 -> 运算符
    T* operator->() const {
        return ptr_;
    }

    // 返回原始指针
    T* get() const {
        return ptr_;
    }

    // 返回当前引用计数
    std::size_t use_count() const {
        return ref_count_ ? *ref_count_ : 0;
    }

    // 重置指针
    void reset() {
        release();
    }

    // 重置为新指针
    void reset(T* raw) {
        release();
        if (raw) {
            ptr_ = raw;
            ref_count_ = new std::size_t(1);
        }
    }

    // 支持 bool 转换，判断是否为空
    explicit operator bool() const {
        return ptr_ != nullptr;
    }
};

// ===================== 测试 =====================

struct TestObj {
    int value;
    static int alive_count;
    TestObj(int v) : value(v) { ++alive_count; }
    ~TestObj() { --alive_count; }
};
int TestObj::alive_count = 0;

int main() {
    // 测试1: 基本构造和析构
    {
        SharedPtr<TestObj> p1(new TestObj(42));
        assert(p1.use_count() == 1);
        assert(p1->value == 42);
        assert((*p1).value == 42);
        assert(p1.get() != nullptr);
        assert(TestObj::alive_count == 1);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 基本构造和析构" << std::endl;

    // 测试2: 拷贝构造
    {
        SharedPtr<TestObj> p1(new TestObj(10));
        SharedPtr<TestObj> p2(p1);
        assert(p1.use_count() == 2);
        assert(p2.use_count() == 2);
        assert(p1.get() == p2.get());
        assert(TestObj::alive_count == 1);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 拷贝构造" << std::endl;

    // 测试3: 拷贝赋值
    {
        SharedPtr<TestObj> p1(new TestObj(20));
        SharedPtr<TestObj> p2(new TestObj(30));
        assert(TestObj::alive_count == 2);
        p2 = p1;
        assert(p1.use_count() == 2);
        assert(p2.use_count() == 2);
        assert(p1.get() == p2.get());
        assert(TestObj::alive_count == 1); // 30 的对象已被释放
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 拷贝赋值" << std::endl;

    // 测试4: 移动构造
    {
        SharedPtr<TestObj> p1(new TestObj(50));
        TestObj* raw = p1.get();
        SharedPtr<TestObj> p2(std::move(p1));
        assert(p1.get() == nullptr);
        assert(p1.use_count() == 0);
        assert(p2.get() == raw);
        assert(p2.use_count() == 1);
        assert(TestObj::alive_count == 1);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 移动构造" << std::endl;

    // 测试5: 移动赋值
    {
        SharedPtr<TestObj> p1(new TestObj(60));
        SharedPtr<TestObj> p2(new TestObj(70));
        TestObj* raw = p1.get();
        p2 = std::move(p1);
        assert(p1.get() == nullptr);
        assert(p1.use_count() == 0);
        assert(p2.get() == raw);
        assert(p2.use_count() == 1);
        assert(TestObj::alive_count == 1); // 70 的对象被释放
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 移动赋值" << std::endl;

    // 测试6: reset
    {
        SharedPtr<TestObj> p1(new TestObj(80));
        SharedPtr<TestObj> p2(p1);
        assert(p1.use_count() == 2);
        p1.reset();
        assert(p1.get() == nullptr);
        assert(p1.use_count() == 0);
        assert(p2.use_count() == 1);
        assert(TestObj::alive_count == 1);

        p2.reset(new TestObj(90));
        assert(TestObj::alive_count == 1); // 80 释放，90 新建
        assert(p2->value == 90);
        assert(p2.use_count() == 1);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] reset" << std::endl;

    // 测试7: 空指针
    {
        SharedPtr<TestObj> p1;
        assert(p1.get() == nullptr);
        assert(p1.use_count() == 0);
        assert(!p1);

        SharedPtr<TestObj> p2(nullptr);
        assert(p2.get() == nullptr);
        assert(p2.use_count() == 0);
    }
    std::cout << "[PASS] 空指针处理" << std::endl;

    // 测试8: 多个指针共享同一对象
    {
        SharedPtr<TestObj> p1(new TestObj(100));
        SharedPtr<TestObj> p2(p1);
        SharedPtr<TestObj> p3(p2);
        SharedPtr<TestObj> p4;
        p4 = p3;
        assert(p1.use_count() == 4);
        assert(p2.use_count() == 4);
        assert(p3.use_count() == 4);
        assert(p4.use_count() == 4);
        assert(TestObj::alive_count == 1);

        p2.reset();
        assert(p1.use_count() == 3);

        p3 = SharedPtr<TestObj>(); // 赋值为空
        assert(p1.use_count() == 2);

        assert(TestObj::alive_count == 1);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 多指针共享同一对象" << std::endl;

    // 测试9: 自赋值
    {
        SharedPtr<TestObj> p1(new TestObj(200));
        p1 = p1;
        assert(p1.use_count() == 1);
        assert(p1->value == 200);
    }
    assert(TestObj::alive_count == 0);
    std::cout << "[PASS] 自赋值" << std::endl;

    std::cout << "\n所有测试通过！" << std::endl;
    return 0;
}
