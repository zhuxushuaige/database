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
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "common/context.h"
#include "common/config.h"

#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
    static constexpr size_t COL_WIDTH = 16;
    size_t num_cols;
    QueryResultWriter *result_writer_ = nullptr;
    std::vector<ColMeta> result_columns_;
    size_t result_rows_ = 0;
    bool result_body_started_ = false;
    bool result_ended_ = false;
public:
    RecordPrinter(size_t num_cols_) : num_cols(num_cols_) {
        assert(num_cols_ > 0);
    }

    void print_separator(Context *context) {
        if (context->result_writer_ != nullptr) {
            result_writer_ = context->result_writer_;
            if (!result_columns_.empty()) {
                if (!result_body_started_) {
                    result_body_started_ = true;
                } else if (!result_ended_) {
                    result_writer_->EndResult(result_rows_);
                    result_ended_ = true;
                }
            }
            return;
        }
        for (size_t i = 0; i < num_cols; i++) {
            // std::cout << '+' << std::string(COL_WIDTH + 2, '-');
            std::string str = "+" + std::string(COL_WIDTH + 2, '-');
            if(context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
                memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
                *(context->offset_) = *(context->offset_) + str.length();
            }
            else {
                context->ellipsis_ = true;
            }
        }
        std::string str = "+\n";
        if(context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
            *(context->offset_) = *(context->offset_) + str.length();
        }
        else {
            context->ellipsis_ = true;
        }
    }

    void print_record(const std::vector<std::string> &rec_str, Context *context) {
        assert(rec_str.size() == num_cols);
        if (context->result_writer_ != nullptr) {
            result_writer_ = context->result_writer_;
            if (result_columns_.empty()) {
                result_columns_.reserve(rec_str.size());
                for (const auto &name : rec_str) {
                    result_columns_.push_back({"", name, TYPE_STRING, 0, 0, false});
                }
                result_writer_->BeginResult(result_columns_);
                return;
            }
            std::vector<ColMeta> row_columns = result_columns_;
            std::vector<const char *> cells;
            cells.reserve(rec_str.size());
            for (size_t i = 0; i < rec_str.size(); ++i) {
                row_columns[i].len = static_cast<int>(rec_str[i].size());
                cells.push_back(rec_str[i].data());
            }
            result_writer_->WriteRow(row_columns, cells);
            ++result_rows_;
            return;
        }
        if (context->ellipsis_) {
            return;
        }
        std::stringstream row;
        for (auto col: rec_str) {
            if (col.size() > COL_WIDTH) {
                col = col.substr(0, COL_WIDTH - 3) + "...";
            }
            row << "| " << std::setw(COL_WIDTH) << col << " ";
        }
        row << "|\n";
        std::string str = row.str();
        if (*context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
            *(context->offset_) += str.length();
        } else {
            context->ellipsis_ = true;
        }
    }

    static void print_record_count(size_t num_rec, Context *context) {
        if (context->result_writer_ != nullptr) {
            return;
        }
        // std::cout << "Total record(s): " << num_rec << '\n';
        std::string str = "";
        if(context->ellipsis_ == true) {
            str = "... ...\n";
        }
        str += "Total record(s): " + std::to_string(num_rec) + '\n';
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) = *(context->offset_) + str.length();
    }
};
