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

#include <algorithm>
#include <set>
#include <shared_mutex>

#include "common/common.h"
#include "execution_defs.h"
#include "common/common.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class VisibleTupleRef {
   public:
    enum class Kind : rmdb::u8 { Empty, BorrowedCurrent, IndexKeyView, OwnedHistorical };

    VisibleTupleRef() = default;
    VisibleTupleRef(const VisibleTupleRef &) = delete;
    VisibleTupleRef &operator=(const VisibleTupleRef &) = delete;
    VisibleTupleRef(VisibleTupleRef &&) noexcept = default;
    VisibleTupleRef &operator=(VisibleTupleRef &&) noexcept = default;

    static VisibleTupleRef Borrowed(ReadPageGuard page_guard, const char *data, int size) {
        return Borrowed(std::move(page_guard), {}, data, size);
    }

    static VisibleTupleRef Borrowed(ReadPageGuard page_guard,
                                    std::shared_lock<std::shared_mutex> record_guard,
                                    const char *data, int size) {
        VisibleTupleRef result;
        if (!page_guard || data == nullptr || size < 0) return result;
        result.page_guard_ = std::move(page_guard);
        result.record_guard_ = std::move(record_guard);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    static VisibleTupleRef Borrowed(std::shared_lock<std::shared_mutex> record_guard,
                                    const char *data, int size) {
        VisibleTupleRef result;
        if (!record_guard.owns_lock() || data == nullptr || size < 0) return result;
        result.record_guard_ = std::move(record_guard);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    static VisibleTupleRef Borrowed(PageReadLatchGuard page_latch, const char *data, int size) {
        VisibleTupleRef result;
        if (!page_latch || data == nullptr || size < 0) return result;
        result.page_latch_ = std::move(page_latch);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    static VisibleTupleRef IndexKey(ReadPageGuard page_guard, const char *data, int size) {
        VisibleTupleRef result = Borrowed(std::move(page_guard), data, size);
        if (result) result.kind_ = Kind::IndexKeyView;
        return result;
    }

    static VisibleTupleRef IndexKeyBorrowed(const char *data, int size) {
        VisibleTupleRef result;
        if (data == nullptr || size < 0) return result;
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::IndexKeyView;
        return result;
    }

    static VisibleTupleRef Owned(std::unique_ptr<RmRecord> record) {
        VisibleTupleRef result;
        if (record == nullptr) return result;
        result.owner_ = std::move(record);
        result.data_ = result.owner_->data;
        result.size_ = result.owner_->size;
        result.kind_ = Kind::OwnedHistorical;
        return result;
    }

    explicit operator bool() const noexcept { return kind_ != Kind::Empty && data_ != nullptr; }
    Kind kind() const noexcept { return kind_; }
    bool borrowed() const noexcept { return kind_ == Kind::BorrowedCurrent || kind_ == Kind::IndexKeyView; }
    bool index_key() const noexcept { return kind_ == Kind::IndexKeyView; }
    bool owned() const noexcept { return kind_ == Kind::OwnedHistorical; }
    const char *data() const noexcept { return data_; }
    int size() const noexcept { return size_; }

   private:
    Kind kind_ = Kind::Empty;
    ReadPageGuard page_guard_;
    PageReadLatchGuard page_latch_;
    std::shared_lock<std::shared_mutex> record_guard_;
    std::unique_ptr<RmRecord> owner_;
    const char *data_ = nullptr;
    int size_ = 0;
};

class AbstractExecutor {
   public:
    struct TupleView {
        const RmRecord *record = nullptr;
        const std::vector<const char *> *cells = nullptr;
        const char *raw_data = nullptr;
        int raw_size = 0;

        explicit operator bool() const { return record != nullptr || cells != nullptr || raw_data != nullptr; }

        const char *cell_at(const ColMeta &col, size_t idx) const {
            if (cells != nullptr) {
                if (idx >= cells->size() || (*cells)[idx] == nullptr) {
                    throw RMDBError("invalid tuple view cell");
                }
                return (*cells)[idx];
            }
            if (record == nullptr || record->data == nullptr || col.offset < 0 || col.len < 0 ||
                col.offset + col.len > record->size) {
                if (raw_data == nullptr || col.offset < 0 || col.len < 0 || col.offset + col.len > raw_size) {
                    throw RMDBError("invalid tuple record view");
                }
                return raw_data + col.offset;
            }
            return record->data + col.offset;
        }
    };

    struct TupleViewRef {
        enum class Ownership { Empty, Borrowed, Owned, Visible };

        TupleViewRef() = default;
        TupleViewRef(const TupleViewRef &) = delete;
        TupleViewRef &operator=(const TupleViewRef &) = delete;

        TupleViewRef(TupleViewRef &&other) noexcept { move_from(std::move(other)); }

        TupleViewRef &operator=(TupleViewRef &&other) noexcept {
            if (this != &other) {
                move_from(std::move(other));
            }
            return *this;
        }

        static TupleViewRef Borrowed(const TupleView *view) {
            TupleViewRef result;
            result.view_ = view;
            result.ownership_ = view == nullptr ? Ownership::Empty : Ownership::Borrowed;
            return result;
        }

        static TupleViewRef Owned(std::unique_ptr<RmRecord> owner) {
            TupleViewRef result;
            if (owner == nullptr) {
                return result;
            }
            result.owner_ = std::move(owner);
            result.owned_view_.record = result.owner_.get();
            result.owned_view_.cells = nullptr;
            result.view_ = &result.owned_view_;
            result.ownership_ = Ownership::Owned;
            return result;
        }

        static TupleViewRef Visible(VisibleTupleRef ref) {
            TupleViewRef result;
            if (!ref) {
                return result;
            }
            result.visible_ = std::move(ref);
            result.owned_view_.raw_data = result.visible_.data();
            result.owned_view_.raw_size = result.visible_.size();
            result.view_ = &result.owned_view_;
            result.ownership_ = Ownership::Visible;
            return result;
        }

        explicit operator bool() const { return view_ != nullptr && static_cast<bool>(*view_); }

        const TupleView &operator*() const { return *view_; }
        const TupleView *operator->() const { return view_; }

        Ownership ownership() const { return ownership_; }
        bool owns_record() const { return ownership_ == Ownership::Owned; }
        bool owns_visible_lease() const { return ownership_ == Ownership::Visible; }

       private:
        void move_from(TupleViewRef &&other) {
            ownership_ = other.ownership_;
            owner_ = std::move(other.owner_);
            visible_ = std::move(other.visible_);
            owned_view_ = other.owned_view_;
            if (ownership_ == Ownership::Owned) {
                owned_view_.record = owner_.get();
                view_ = &owned_view_;
            } else if (ownership_ == Ownership::Visible) {
                owned_view_.raw_data = visible_ ? visible_.data() : nullptr;
                owned_view_.raw_size = visible_ ? visible_.size() : 0;
                view_ = &owned_view_;
            } else {
                view_ = other.view_;
            }
            other.view_ = nullptr;
            other.owned_view_ = {};
            other.ownership_ = Ownership::Empty;
        }

        const TupleView *view_{nullptr};
        std::unique_ptr<RmRecord> owner_;
        VisibleTupleRef visible_;
        TupleView owned_view_;
        Ownership ownership_{Ownership::Empty};
    };

    Rid _abstract_rid;

    Context *context_{nullptr};

    enum class CompiledComparator : rmdb::u8 {
        kInvalid,
        kIntEq, kIntNe, kIntLt, kIntGt, kIntLe, kIntGe,
        kFloatEq, kFloatNe, kFloatLt, kFloatGt, kFloatLe, kFloatGe,
        kBytesEq, kBytesNe, kBytesLt, kBytesGt, kBytesLe, kBytesGe,
    };

    static CompiledComparator compile_comparator(ColType type, CompOp op) {
        if (type == TYPE_INT) {
            switch (op) {
                case OP_EQ: return CompiledComparator::kIntEq;
                case OP_NE: return CompiledComparator::kIntNe;
                case OP_LT: return CompiledComparator::kIntLt;
                case OP_GT: return CompiledComparator::kIntGt;
                case OP_LE: return CompiledComparator::kIntLe;
                case OP_GE: return CompiledComparator::kIntGe;
            }
        }
        if (type == TYPE_FLOAT) {
            switch (op) {
                case OP_EQ: return CompiledComparator::kFloatEq;
                case OP_NE: return CompiledComparator::kFloatNe;
                case OP_LT: return CompiledComparator::kFloatLt;
                case OP_GT: return CompiledComparator::kFloatGt;
                case OP_LE: return CompiledComparator::kFloatLe;
                case OP_GE: return CompiledComparator::kFloatGe;
            }
        }
        switch (op) {
            case OP_EQ: return CompiledComparator::kBytesEq;
            case OP_NE: return CompiledComparator::kBytesNe;
            case OP_LT: return CompiledComparator::kBytesLt;
            case OP_GT: return CompiledComparator::kBytesGt;
            case OP_LE: return CompiledComparator::kBytesLe;
            case OP_GE: return CompiledComparator::kBytesGe;
        }
        return CompiledComparator::kInvalid;
    }

    static bool eval_compiled_comparator(CompiledComparator comparator, const char *lhs,
                                         const char *rhs, int len) {
        switch (comparator) {
            case CompiledComparator::kIntEq:
                return rmdb::load_unaligned<int>(lhs) == rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kIntNe:
                return rmdb::load_unaligned<int>(lhs) != rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kIntLt:
                return rmdb::load_unaligned<int>(lhs) < rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kIntGt:
                return rmdb::load_unaligned<int>(lhs) > rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kIntLe:
                return rmdb::load_unaligned<int>(lhs) <= rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kIntGe:
                return rmdb::load_unaligned<int>(lhs) >= rmdb::load_unaligned<int>(rhs);
            case CompiledComparator::kFloatEq:
                return rmdb::load_unaligned<float>(lhs) == rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kFloatNe:
                return rmdb::load_unaligned<float>(lhs) != rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kFloatLt:
                return rmdb::load_unaligned<float>(lhs) < rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kFloatGt:
                return rmdb::load_unaligned<float>(lhs) > rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kFloatLe:
                return rmdb::load_unaligned<float>(lhs) <= rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kFloatGe:
                return rmdb::load_unaligned<float>(lhs) >= rmdb::load_unaligned<float>(rhs);
            case CompiledComparator::kBytesEq: return memcmp(lhs, rhs, len) == 0;
            case CompiledComparator::kBytesNe: return memcmp(lhs, rhs, len) != 0;
            case CompiledComparator::kBytesLt: return memcmp(lhs, rhs, len) < 0;
            case CompiledComparator::kBytesGt: return memcmp(lhs, rhs, len) > 0;
            case CompiledComparator::kBytesLe: return memcmp(lhs, rhs, len) <= 0;
            case CompiledComparator::kBytesGe: return memcmp(lhs, rhs, len) >= 0;
            case CompiledComparator::kInvalid: return false;
        }
        return false;
    }

    struct CompiledCondition {
        size_t lhs_idx = 0;
        size_t rhs_idx = 0;
        ColMeta lhs_col;
        ColMeta rhs_col;
        CompOp op = OP_EQ;
        const char *rhs_value = nullptr;
        bool rhs_is_value = false;
        CompiledComparator comparator = CompiledComparator::kInvalid;
    };

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual const RmRecord *CurrentTuple() const { return nullptr; }

    virtual const TupleView *CurrentTupleView() const {
        const RmRecord *record = CurrentTuple();
        if (record == nullptr) {
            return nullptr;
        }
        fallback_tuple_view_.record = record;
        fallback_tuple_view_.cells = nullptr;
        return &fallback_tuple_view_;
    }

    TupleViewRef ReadTupleView() {
        if (context_ != nullptr) {
            context_->check_execution_deadline_periodic();
        }
        const TupleView *view = CurrentTupleView();
        if (view != nullptr) {
            return TupleViewRef::Borrowed(view);
        }
        return TupleViewRef::Owned(Next());
    }

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) const {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    size_t get_col_index(const std::vector<ColMeta> &rec_cols, const TabCol &target) const {
        auto pos = get_col(rec_cols, target);
        return static_cast<size_t>(pos - rec_cols.begin());
    }

    int compare_value(const char *lhs, const char *rhs, ColType type, int len) const {
        if (type == TYPE_INT) {
            int a = rmdb::load_unaligned<int>(lhs);
            int b = rmdb::load_unaligned<int>(rhs);
            return (a > b) - (a < b);
        }
        if (type == TYPE_FLOAT) {
            float a = rmdb::load_unaligned<float>(lhs);
            float b = rmdb::load_unaligned<float>(rhs);
            return (a > b) - (a < b);
        }
        return memcmp(lhs, rhs, len);
    }

    bool compare_result(int cmp, CompOp op) const {
        switch (op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
        }
        return false;
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const RmRecord *rec,
                    const std::vector<Condition> &conds) const {
        for (const auto &cond : conds) {
            auto lhs_col = get_col(rec_cols, cond.lhs_col);
            const char *lhs = rec->data + lhs_col->offset;
            const char *rhs = nullptr;
            if (cond.is_rhs_val) {
                rhs = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                rhs = rec->data + rhs_col->offset;
            }
            if (!compare_result(compare_value(lhs, rhs, lhs_col->type, lhs_col->len), cond.op)) {
                return false;
            }
        }
        return true;
    }

    bool eval_conds_view(const std::vector<ColMeta> &rec_cols, const TupleView &view,
                         const std::vector<Condition> &conds) const {
        for (const auto &cond : conds) {
            auto lhs_col = get_col(rec_cols, cond.lhs_col);
            size_t lhs_idx = static_cast<size_t>(lhs_col - rec_cols.begin());
            const char *lhs = view.cell_at(*lhs_col, lhs_idx);
            const char *rhs = nullptr;
            if (cond.is_rhs_val) {
                rhs = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                size_t rhs_idx = static_cast<size_t>(rhs_col - rec_cols.begin());
                rhs = view.cell_at(*rhs_col, rhs_idx);
            }
            if (!compare_result(compare_value(lhs, rhs, lhs_col->type, lhs_col->len), cond.op)) {
                return false;
            }
        }
        return true;
    }

    bool compile_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds,
                       std::vector<CompiledCondition> *compiled) const {
        compiled->clear();
        compiled->reserve(conds.size());
        for (const auto &cond : conds) {
            auto lhs_col = get_col(rec_cols, cond.lhs_col);
            CompiledCondition item;
            item.lhs_idx = static_cast<size_t>(lhs_col - rec_cols.begin());
            item.lhs_col = *lhs_col;
            item.op = cond.op;
            item.rhs_is_value = cond.is_rhs_val;
            item.comparator = compile_comparator(lhs_col->type, cond.op);
            if (item.comparator == CompiledComparator::kInvalid) {
                compiled->clear();
                return false;
            }
            if (cond.is_rhs_val) {
                if (cond.rhs_val.raw == nullptr) {
                    compiled->clear();
                    return false;
                }
                item.rhs_value = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                if (rhs_col->type != lhs_col->type || rhs_col->len != lhs_col->len) {
                    compiled->clear();
                    return false;
                }
                item.rhs_idx = static_cast<size_t>(rhs_col - rec_cols.begin());
                item.rhs_col = *rhs_col;
            }
            compiled->push_back(item);
        }
        return true;
    }

    bool eval_compiled_conds_view(const TupleView &view, const std::vector<CompiledCondition> &conds) const {
        for (const auto &cond : conds) {
            const char *lhs = view.cell_at(cond.lhs_col, cond.lhs_idx);
            const char *rhs = cond.rhs_is_value ? cond.rhs_value : view.cell_at(cond.rhs_col, cond.rhs_idx);
            if (!eval_compiled_comparator(cond.comparator, lhs, rhs, cond.lhs_col.len)) {
                return false;
            }
        }
        return true;
    }

    bool eval_compiled_conds_record(const RmRecord *rec, const std::vector<CompiledCondition> &conds) const {
        for (const auto &cond : conds) {
            const char *lhs = rec->data + cond.lhs_col.offset;
            const char *rhs = cond.rhs_is_value ? cond.rhs_value : rec->data + cond.rhs_col.offset;
            if (!eval_compiled_comparator(cond.comparator, lhs, rhs, cond.lhs_col.len)) {
                return false;
            }
        }
        return true;
    }

    void materialize_tuple_view(const TupleView &view, const std::vector<ColMeta> &rec_cols, RmRecord *out,
                                size_t tuple_len) const {
        out->Resize(static_cast<int>(tuple_len));
        if (view.cells == nullptr && view.record != nullptr && view.record->data != nullptr &&
            static_cast<size_t>(view.record->size) >= tuple_len) {
            memcpy(out->data, view.record->data, tuple_len);
            return;
        }
        for (size_t i = 0; i < rec_cols.size(); ++i) {
            const auto &col = rec_cols[i];
            if (col.offset < 0 || col.len < 0 || static_cast<size_t>(col.offset + col.len) > tuple_len) {
                throw RMDBError("invalid materialized tuple column");
            }
            memcpy(out->data + col.offset, view.cell_at(col, i), col.len);
        }
    }

    void build_column_projection(const std::vector<ColMeta> &full_cols, const std::vector<TabCol> &required_cols,
                                 std::vector<ColMeta> *out_cols, std::vector<ColMeta> *source_cols,
                                 size_t *out_len) const {
        out_cols->clear();
        source_cols->clear();
        *out_len = 0;
        std::set<TabCol> seen;
        for (const auto &required : required_cols) {
            if (!seen.insert(required).second) {
                continue;
            }
            auto source = get_col(full_cols, required);
            ColMeta out_col = *source;
            out_col.offset = static_cast<int>(*out_len);
            *out_len += static_cast<size_t>(out_col.len);
            out_cols->push_back(out_col);
            source_cols->push_back(*source);
        }
    }

   private:
    mutable TupleView fallback_tuple_view_;
};

/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. */

#include <memory>
#if 0  // VisibleTupleRef moved above AbstractExecutor after consolidation.
#include <shared_mutex>
#include <utility>

#include "record/rm_defs.h"
#include "common/common.h"
#include "storage/page.h"

/**
 * Lifetime-safe result of a visibility lookup.
 *
 * Borrowed references keep a buffer-page read lease alongside the raw tuple
 * bytes.  Historical versions are owned because they are reconstructed from
 * undo state and cannot safely point into a mutable page.  The type is kept
 * independent of any particular scan executor so SeqScan, IndexScan and
 * aggregate fast paths can share the same ownership contract.
 */
class VisibleTupleRef {
   public:
    enum class Kind : rmdb::u8 { Empty, BorrowedCurrent, IndexKeyView, OwnedHistorical };

    VisibleTupleRef() = default;
    VisibleTupleRef(const VisibleTupleRef &) = delete;
    VisibleTupleRef &operator=(const VisibleTupleRef &) = delete;
    VisibleTupleRef(VisibleTupleRef &&) noexcept = default;
    VisibleTupleRef &operator=(VisibleTupleRef &&) noexcept = default;

    static VisibleTupleRef Borrowed(ReadPageGuard page_guard, const char *data, int size) {
        return Borrowed(std::move(page_guard), {}, data, size);
    }

    static VisibleTupleRef Borrowed(ReadPageGuard page_guard,
                                    std::shared_lock<std::shared_mutex> record_guard,
                                    const char *data, int size) {
        VisibleTupleRef result;
        if (!page_guard || data == nullptr || size < 0) {
            return result;
        }
        result.page_guard_ = std::move(page_guard);
        result.record_guard_ = std::move(record_guard);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    static VisibleTupleRef Borrowed(std::shared_lock<std::shared_mutex> record_guard, const char *data, int size) {
        VisibleTupleRef result;
        if (!record_guard.owns_lock() || data == nullptr || size < 0) {
            return result;
        }
        result.record_guard_ = std::move(record_guard);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    // The owning RmScan keeps the page pinned. This lease only extends the
    // page read latch until the executor advances the scan.
    static VisibleTupleRef Borrowed(PageReadLatchGuard page_latch, const char *data, int size) {
        VisibleTupleRef result;
        if (!page_latch || data == nullptr || size < 0) {
            return result;
        }
        result.page_latch_ = std::move(page_latch);
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::BorrowedCurrent;
        return result;
    }

    static VisibleTupleRef IndexKey(ReadPageGuard page_guard, const char *data, int size) {
        VisibleTupleRef result = Borrowed(std::move(page_guard), data, size);
        if (result) {
            result.kind_ = Kind::IndexKeyView;
        }
        return result;
    }

    // The owning IxScan keeps its current leaf guard until nextTuple(). The
    // caller must consume this view before advancing that scan.
    static VisibleTupleRef IndexKeyBorrowed(const char *data, int size) {
        VisibleTupleRef result;
        if (data == nullptr || size < 0) {
            return result;
        }
        result.data_ = data;
        result.size_ = size;
        result.kind_ = Kind::IndexKeyView;
        return result;
    }

    static VisibleTupleRef Owned(std::unique_ptr<RmRecord> record) {
        VisibleTupleRef result;
        if (record == nullptr) {
            return result;
        }
        result.owner_ = std::move(record);
        result.data_ = result.owner_->data;
        result.size_ = result.owner_->size;
        result.kind_ = Kind::OwnedHistorical;
        return result;
    }

    explicit operator bool() const noexcept { return kind_ != Kind::Empty && data_ != nullptr; }
    Kind kind() const noexcept { return kind_; }
    bool borrowed() const noexcept { return kind_ == Kind::BorrowedCurrent || kind_ == Kind::IndexKeyView; }
    bool index_key() const noexcept { return kind_ == Kind::IndexKeyView; }
    bool owned() const noexcept { return kind_ == Kind::OwnedHistorical; }
    const char *data() const noexcept { return data_; }
    int size() const noexcept { return size_; }

   private:
    Kind kind_ = Kind::Empty;
    ReadPageGuard page_guard_;
    PageReadLatchGuard page_latch_;
    std::shared_lock<std::shared_mutex> record_guard_;
    std::unique_ptr<RmRecord> owner_;
    const char *data_ = nullptr;
    int size_ = 0;
};
#endif

