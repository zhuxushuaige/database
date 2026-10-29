/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction/watermark.h"


auto Watermark::AddTxn(timestamp_t read_ts) -> void {
    std::lock_guard<std::mutex> guard(latch_);
    current_reads_.push_back(read_ts);
    if (current_reads_.size() == 1 || read_ts < watermark_) {
        watermark_ = read_ts;
    }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
    std::lock_guard<std::mutex> guard(latch_);
    auto iter = std::find(current_reads_.begin(), current_reads_.end(), read_ts);
    if (iter == current_reads_.end()) {
        return;
    }
    *iter = current_reads_.back();
    current_reads_.pop_back();
    watermark_ = current_reads_.empty()
                     ? commit_ts_
                     : *std::min_element(current_reads_.begin(), current_reads_.end());
}

auto Watermark::UpdateCommitTs(timestamp_t commit_ts) -> void {
    std::lock_guard<std::mutex> guard(latch_);
    commit_ts_ = commit_ts;
    if (current_reads_.empty()) {
        watermark_ = commit_ts_;
    }
}

auto Watermark::FinishTxn(timestamp_t read_ts, timestamp_t commit_ts) -> void {
    std::lock_guard<std::mutex> guard(latch_);
    commit_ts_ = commit_ts;
    auto iter = std::find(current_reads_.begin(), current_reads_.end(), read_ts);
    if (iter != current_reads_.end()) {
        *iter = current_reads_.back();
        current_reads_.pop_back();
    }
    watermark_ = current_reads_.empty()
                     ? commit_ts_
                     : *std::min_element(current_reads_.begin(), current_reads_.end());
}

auto Watermark::GetWatermark() -> timestamp_t {
    std::lock_guard<std::mutex> guard(latch_);
    return watermark_;
}
