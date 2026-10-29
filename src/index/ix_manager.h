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

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "ix_defs.h"
#include "ix_index_handle.h"
#include "system/sm_meta.h"

class IxManager {
   public:
    IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    [[nodiscard]] std::string get_index_name(const std::string &table,
                                             const std::vector<std::string> &columns) const {
        return compose_name(table, columns, [](const std::string &column) -> const std::string & { return column; });
    }

    [[nodiscard]] std::string get_index_name(const std::string &table,
                                             const std::vector<ColMeta> &columns) const {
        return compose_name(table, columns, [](const ColMeta &column) -> const std::string & { return column.name; });
    }

    [[nodiscard]] std::string get_index_name(const std::string &table, const IndexMeta &index) const {
        return index.index_name.empty() ? get_index_name(table, index.cols) : index.index_name;
    }

    [[nodiscard]] bool exists(const std::string &table, const std::vector<ColMeta> &columns) const {
        return index_file_exists(get_index_name(table, columns));
    }

    [[nodiscard]] bool exists(const std::string &table, const std::vector<std::string> &columns) const {
        return index_file_exists(get_index_name(table, columns));
    }

    [[nodiscard]] bool exists(const std::string &table, const IndexMeta &index) const {
        return index_file_exists(get_index_name(table, index));
    }

    void create_index(const std::string &table, const std::vector<ColMeta> &columns) {
        IndexMeta index;
        index.tab_name = table;
        index.index_name = get_index_name(table, columns);
        index.col_num = static_cast<int>(columns.size());
        index.cols = columns;
        index.col_tot_len = index.logical_col_tot_len();
        index.unique = true;
        index.hidden = false;
        create_index(table, index);
    }

    void create_index(const std::string &table, const IndexMeta &index) {
        const std::string path = get_index_name(table, index);
        const PhysicalLayout layout = make_layout(index);
        disk_manager_->create_file(path);
        const int fd = disk_manager_->open_file(path);
        try {
            write_header(fd, layout);
            write_leaf_page(fd, IX_LEAF_HEADER_PAGE, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
            write_leaf_page(fd, IX_INIT_ROOT_PAGE, IX_LEAF_HEADER_PAGE, IX_LEAF_HEADER_PAGE);
            disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES - 1);
            disk_manager_->close_file(fd);
        } catch (...) {
            disk_manager_->close_file(fd);
            throw;
        }
    }

    void destroy_index(const std::string &table, const std::vector<ColMeta> &columns) {
        destroy_named(get_index_name(table, columns));
    }

    void destroy_index(const std::string &table, const std::vector<std::string> &columns) {
        destroy_named(get_index_name(table, columns));
    }

    void destroy_index(const std::string &table, const IndexMeta &index) {
        destroy_named(get_index_name(table, index));
    }

    [[nodiscard]] std::unique_ptr<IxIndexHandle> open_index(const std::string &table,
                                                            const std::vector<ColMeta> &columns) {
        return open_named(get_index_name(table, columns));
    }

    [[nodiscard]] std::unique_ptr<IxIndexHandle> open_index(const std::string &table,
                                                            const std::vector<std::string> &columns) {
        return open_named(get_index_name(table, columns));
    }

    [[nodiscard]] std::unique_ptr<IxIndexHandle> open_index(const std::string &table, const IndexMeta &index) {
        return open_named(get_index_name(table, index));
    }

    void close_index(IxIndexHandle *handle) {
        if (handle == nullptr) {
            return;
        }
        handle->flush();
        disk_manager_->sync_file(handle->fd_);
        disk_manager_->close_file(handle->fd_);
    }

   private:
    struct PhysicalLayout {
        std::vector<ColType> types;
        std::vector<int> lengths;
        int key_length{0};
        int order{0};
    };

    template <typename Range, typename NameOf>
    static std::string compose_name(const std::string &table, const Range &columns, NameOf name_of) {
        size_t result_size = table.size() + sizeof(".idx") - 1;
        for (const auto &column : columns) {
            result_size += 1 + name_of(column).size();
        }
        std::string result;
        result.reserve(result_size);
        result.append(table);
        for (const auto &column : columns) {
            result.push_back('_');
            result.append(name_of(column));
        }
        result.append(".idx");
        return result;
    }

    static PhysicalLayout make_layout(const IndexMeta &index) {
        PhysicalLayout layout;
        const size_t hidden_columns = index.unique ? 0 : 2;
        layout.types.reserve(index.cols.size() + hidden_columns);
        layout.lengths.reserve(index.cols.size() + hidden_columns);
        for (const ColMeta &column : index.cols) {
            layout.types.push_back(column.type);
            layout.lengths.push_back(column.len);
            layout.key_length += column.len;
        }
        if (!index.unique) {
            layout.types.insert(layout.types.end(), {TYPE_INT, TYPE_INT});
            layout.lengths.insert(layout.lengths.end(), {static_cast<int>(sizeof(int)), static_cast<int>(sizeof(int))});
            layout.key_length += static_cast<int>(sizeof(Rid));
        }

        const int maximum = index.unique ? IX_MAX_COL_LEN : IX_MAX_COL_LEN + static_cast<int>(sizeof(Rid));
        if (layout.key_length > maximum || layout.key_length != index.col_tot_len) {
            throw InvalidColLengthError(layout.key_length);
        }
        layout.order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) /
                                        (layout.key_length + static_cast<int>(sizeof(Rid))) - 1);
        assert(layout.order > 2);
        return layout;
    }

    void write_header(int fd, const PhysicalLayout &layout) {
        IxFileHdr header(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE,
                         static_cast<int>(layout.lengths.size()), layout.key_length, layout.order,
                         (layout.order + 1) * layout.key_length, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
        header.col_types_ = layout.types;
        header.col_lens_ = layout.lengths;
        header.update_tot_len();

        std::vector<char> encoded(static_cast<size_t>(header.tot_len_));
        header.serialize(encoded.data());
        disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, encoded.data(), header.tot_len_);
    }

    void write_leaf_page(int fd, page_id_t page, page_id_t previous, page_id_t next) {
        alignas(IxPageHdr) char bytes[PAGE_SIZE]{};
        auto *header = reinterpret_cast<IxPageHdr *>(bytes);
        *header = {.next_free_page_no = IX_NO_PAGE,
                   .parent = IX_NO_PAGE,
                   .num_key = 0,
                   .is_leaf = true,
                   .prev_leaf = previous,
                   .next_leaf = next};
        disk_manager_->write_page(fd, page, bytes, PAGE_SIZE);
    }

    [[nodiscard]] bool index_file_exists(const std::string &path) const { return disk_manager_->is_file(path); }

    void destroy_named(const std::string &path) { disk_manager_->destroy_file(path); }

    [[nodiscard]] std::unique_ptr<IxIndexHandle> open_named(const std::string &path) {
        const int fd = disk_manager_->open_file(path);
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
    }

    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
};
