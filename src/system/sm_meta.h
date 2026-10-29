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
#include <iostream>
#include <map>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "errors.h"
#include "common/common.h"
#include "sm_defs.h"

/* 字段元数据 */
struct ColMeta {
    std::string tab_name;   // 字段所属表名称
    std::string name;       // 字段名称
    ColType type;           // 字段类型
    int len;                // 字段长度
    int offset;             // 字段位于记录中的偏移量
    bool index;             /** unused */

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
    }
};

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;           // 索引所属表名称
    std::string index_name;         // 物理索引文件名；旧元数据为空时按表名+列名推导
    rmdb::u64 index_id = 0;          // 数据库实例内稳定的 catalog 索引标识
    int col_tot_len = 0;            // 物理索引键长度，内部非唯一索引包含隐藏RID后缀
    int col_num = 0;                // 索引字段数量
    std::vector<ColMeta> cols;      // 索引包含的字段
    bool unique = true;             // 用户可见CREATE INDEX保持唯一索引语义
    bool hidden = false;            // 内部索引对show/drop index不可见

    [[nodiscard]] int logical_col_tot_len() const {
        return std::accumulate(cols.begin(), cols.end(), 0,
                               [](int total, const ColMeta &col) { return total + col.len; });
    }

    [[nodiscard]] bool matches_cols(const std::vector<std::string> &col_names) const {
        return static_cast<size_t>(col_num) == col_names.size() && cols.size() >= col_names.size() &&
               std::equal(col_names.begin(), col_names.end(), cols.begin(),
                          [](const std::string &name, const ColMeta &col) { return name == col.name; });
    }

    void write_to(std::ostream &os) const;
    void read_from(std::istream &is);

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
        index.write_to(os);
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        index.read_from(is);
        return is;
    }
};

inline void IndexMeta::write_to(std::ostream &os) const {
    os << "@index_meta_v3 " << tab_name << ' ' << (index_name.empty() ? "-" : index_name) << ' ' << index_id << ' '
       << col_tot_len << ' ' << col_num << ' ' << unique << ' ' << hidden;
    for (const ColMeta &col : cols) {
        os << '\n' << col;
    }
}

inline void IndexMeta::read_from(std::istream &is) {
    *this = IndexMeta{};
    std::string version_or_table;
    is >> version_or_table;
    if (version_or_table == "@index_meta_v3") {
        is >> tab_name >> index_name >> index_id >> col_tot_len >> col_num >> unique >> hidden;
    } else if (version_or_table == "@index_meta_v2") {
        is >> tab_name >> index_name >> col_tot_len >> col_num >> unique >> hidden;
    } else {
        tab_name = std::move(version_or_table);
        is >> col_tot_len >> col_num;
    }
    if (index_name == "-") {
        index_name.clear();
    }
    cols.reserve(static_cast<size_t>(std::max(col_num, 0)));
    for (int i = 0; i < col_num; ++i) {
        ColMeta col;
        is >> col;
        cols.push_back(std::move(col));
    }
}

/* 表元数据 */
struct TabMeta {
    std::string name;                   // 表名称
    std::vector<ColMeta> cols;          // 表包含的字段
    std::vector<IndexMeta> indexes;     // 表上建立的索引

    TabMeta() = default;
    TabMeta(const TabMeta &) = default;
    TabMeta(TabMeta &&) noexcept = default;
    TabMeta &operator=(const TabMeta &) = default;
    TabMeta &operator=(TabMeta &&) noexcept = default;

    /* 判断当前表中是否存在名为col_name的字段 */
    [[nodiscard]] bool is_col(const std::string &col_name) const {
        return find_col(col_name) != cols.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names, bool include_hidden = false) const {
        return std::any_of(indexes.begin(), indexes.end(), [&](const IndexMeta &index) {
            return (include_hidden || !index.hidden) && index.matches_cols(col_names);
        });
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names,
                                                    bool include_hidden = false) {
        auto index = find_index(col_names, false);
        if (index != indexes.end()) return index;
        if (include_hidden && (index = find_index(col_names, true)) != indexes.end()) return index;
        throw IndexNotFoundError(name, col_names);
    }

    std::vector<IndexMeta>::const_iterator get_index_meta(const std::vector<std::string>& col_names,
                                                          bool include_hidden = false) const {
        auto index = find_index(col_names, false);
        if (index != indexes.end()) return index;
        if (include_hidden && (index = find_index(col_names, true)) != indexes.end()) return index;
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
        auto pos = find_col(col_name);
        if (pos == cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    std::vector<ColMeta>::const_iterator get_col(const std::string &col_name) const {
        auto pos = find_col(col_name);
        if (pos == cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        tab = TabMeta{};
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;
        for(size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.push_back(index);
        }
        return is;
    }

   private:
    std::vector<ColMeta>::iterator find_col(const std::string &col_name) {
        return std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    }

    std::vector<ColMeta>::const_iterator find_col(const std::string &col_name) const {
        return std::find_if(cols.cbegin(), cols.cend(), [&](const ColMeta &col) { return col.name == col_name; });
    }

    std::vector<IndexMeta>::iterator find_index(const std::vector<std::string> &col_names, bool hidden) {
        return std::find_if(indexes.begin(), indexes.end(), [&](const IndexMeta &index) {
            return index.hidden == hidden && index.matches_cols(col_names);
        });
    }

    std::vector<IndexMeta>::const_iterator find_index(const std::vector<std::string> &col_names, bool hidden) const {
        return std::find_if(indexes.cbegin(), indexes.cend(), [&](const IndexMeta &index) {
            return index.hidden == hidden && index.matches_cols(col_names);
        });
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
    friend class SmManager;

   private:
    std::string name_;                      // 数据库名称
    std::map<std::string, TabMeta> tabs_;   // 数据库中包含的表

   public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        db_meta.tabs_.clear();
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name] = tab;
        }
        return is;
    }
};
