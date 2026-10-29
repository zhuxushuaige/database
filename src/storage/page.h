/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/config.h"

#include <shared_mutex>

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; }
    bool operator<(const PageId& x) const {
        if(fd < x.fd) return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}";
    }

    inline rmdb::i64 Get() const {
        return (static_cast<rmdb::i64>(fd << 16) | page_no);
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

template <>
struct std::hash<PageId> {
    size_t operator()(const PageId &obj) const { return std::hash<rmdb::i64>()(obj.Get()); }
};

/**
 * @description: Page类声明, Page是RMDB数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class alignas(64) Page {
    friend class BufferPoolManager;

   public:

    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = sizeof(lsn_t);

    inline lsn_t get_page_lsn() { return rmdb::load_unaligned<lsn_t>(get_data() + OFFSET_LSN); }

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

    void r_latch() { page_latch_.lock_shared(); }

    void r_unlatch() { page_latch_.unlock_shared(); }

    void w_latch() { page_latch_.lock(); }

    void w_unlatch() { page_latch_.unlock(); }

   private:
    void reset_memory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** Frequently accessed frame metadata stays together in the first cache line. */
    PageId id_;
    bool is_dirty_ = false;
    std::atomic<int> pin_count_{0};
    std::atomic<rmdb::u64> generation_{0};
    std::atomic<bool> is_replacing_{false};
    std::shared_mutex page_latch_;

    /** The persisted page payload starts on its own cache-line boundary. */
    alignas(64) char data_[PAGE_SIZE] = {};
};

static_assert(alignof(Page) >= 64, "buffer frames must be cache-line aligned");

/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. */

#include <utility>

class BufferPoolManager;

/**
 * A move-only read latch for a page whose pin is owned elsewhere.
 *
 * RmScan keeps the current heap page pinned while it advances.  Borrowed
 * tuple views only need to extend the page-latch lifetime, not acquire a
 * second buffer-pool pin for every tuple.
 */
class PageReadLatchGuard {
   public:
    PageReadLatchGuard() = default;
    explicit PageReadLatchGuard(Page *page) noexcept : page_(page) {
        if (page_ != nullptr) {
            page_->r_latch();
        }
    }
    PageReadLatchGuard(const PageReadLatchGuard &) = delete;
    PageReadLatchGuard &operator=(const PageReadLatchGuard &) = delete;

    PageReadLatchGuard(PageReadLatchGuard &&other) noexcept : page_(other.page_) { other.page_ = nullptr; }
    PageReadLatchGuard &operator=(PageReadLatchGuard &&other) noexcept {
        if (this != &other) {
            Drop();
            page_ = other.page_;
            other.page_ = nullptr;
        }
        return *this;
    }

    ~PageReadLatchGuard() { Drop(); }

    explicit operator bool() const noexcept { return page_ != nullptr; }
    void Drop() noexcept {
        if (page_ != nullptr) {
            page_->r_unlatch();
            page_ = nullptr;
        }
    }

   private:
    Page *page_ = nullptr;
};

/** Move-only write latch for a page whose buffer pin is owned elsewhere. */
class PageWriteLatchGuard {
   public:
    PageWriteLatchGuard() = default;
    explicit PageWriteLatchGuard(Page *page) noexcept : page_(page) {
        if (page_ != nullptr) {
            page_->w_latch();
        }
    }
    PageWriteLatchGuard(const PageWriteLatchGuard &) = delete;
    PageWriteLatchGuard &operator=(const PageWriteLatchGuard &) = delete;

    PageWriteLatchGuard(PageWriteLatchGuard &&other) noexcept : page_(other.page_) { other.page_ = nullptr; }
    PageWriteLatchGuard &operator=(PageWriteLatchGuard &&other) noexcept {
        if (this != &other) {
            Drop();
            page_ = other.page_;
            other.page_ = nullptr;
        }
        return *this;
    }

    ~PageWriteLatchGuard() { Drop(); }

    explicit operator bool() const noexcept { return page_ != nullptr; }
    void Drop() noexcept {
        if (page_ != nullptr) {
            page_->w_unlatch();
            page_ = nullptr;
        }
    }

   private:
    Page *page_ = nullptr;
};

/**
 * A move-only read lease for a buffer-pool page.
 *
 * The lease owns both the buffer pin and the page read latch.  Keeping these
 * lifetimes in one object makes it safe for execution-layer borrowed views to
 * outlive the helper that located a slot, while still allowing the page to be
 * evicted as soon as the view is destroyed.
 */
class ReadPageGuard {
   public:
    ReadPageGuard() = default;
    ReadPageGuard(const ReadPageGuard &) = delete;
    ReadPageGuard &operator=(const ReadPageGuard &) = delete;

    ReadPageGuard(ReadPageGuard &&other) noexcept { move_from(std::move(other)); }
    ReadPageGuard &operator=(ReadPageGuard &&other) noexcept {
        if (this != &other) {
            Drop();
            move_from(std::move(other));
        }
        return *this;
    }

    ~ReadPageGuard() { Drop(); }

    explicit operator bool() const noexcept { return page_ != nullptr; }
    Page *get_page() const noexcept { return page_; }
    const char *get_data() const noexcept { return page_ == nullptr ? nullptr : page_->get_data(); }
    PageId page_id() const noexcept { return page_id_; }

    void Drop() noexcept;

   private:
    friend class BufferPoolManager;
    ReadPageGuard(BufferPoolManager *manager, PageId page_id, Page *page) noexcept
        : manager_(manager), page_id_(page_id), page_(page) {}

    void move_from(ReadPageGuard &&other) noexcept {
        manager_ = other.manager_;
        page_id_ = other.page_id_;
        page_ = other.page_;
        other.manager_ = nullptr;
        other.page_ = nullptr;
        other.page_id_ = PageId{-1, INVALID_PAGE_ID};
    }

    BufferPoolManager *manager_ = nullptr;
    PageId page_id_{-1, INVALID_PAGE_ID};
    Page *page_ = nullptr;
};

/**
 * A move-only write lease for a buffer-pool page.
 *
 * A write lease is dirty only when MarkDirty() is called.  This keeps read
 * paths from accidentally extending the WAL/page-dirty boundary.
 */
class WritePageGuard {
   public:
    WritePageGuard() = default;
    WritePageGuard(const WritePageGuard &) = delete;
    WritePageGuard &operator=(const WritePageGuard &) = delete;

    WritePageGuard(WritePageGuard &&other) noexcept { move_from(std::move(other)); }
    WritePageGuard &operator=(WritePageGuard &&other) noexcept {
        if (this != &other) {
            Drop();
            move_from(std::move(other));
        }
        return *this;
    }

    ~WritePageGuard() { Drop(); }

    explicit operator bool() const noexcept { return page_ != nullptr; }
    Page *get_page() const noexcept { return page_; }
    char *get_data() const noexcept { return page_ == nullptr ? nullptr : page_->get_data(); }
    PageId page_id() const noexcept { return page_id_; }

    void MarkDirty() noexcept { dirty_ = true; }
    bool is_dirty() const noexcept { return dirty_; }
    Page *ReleaseLatchKeepPinned() noexcept;
    void Drop() noexcept;

   private:
    friend class BufferPoolManager;
    WritePageGuard(BufferPoolManager *manager, PageId page_id, Page *page) noexcept
        : manager_(manager), page_id_(page_id), page_(page) {}

    void move_from(WritePageGuard &&other) noexcept {
        manager_ = other.manager_;
        page_id_ = other.page_id_;
        page_ = other.page_;
        dirty_ = other.dirty_;
        other.manager_ = nullptr;
        other.page_ = nullptr;
        other.page_id_ = PageId{-1, INVALID_PAGE_ID};
        other.dirty_ = false;
    }

    BufferPoolManager *manager_ = nullptr;
    PageId page_id_{-1, INVALID_PAGE_ID};
    Page *page_ = nullptr;
    bool dirty_ = false;
};

