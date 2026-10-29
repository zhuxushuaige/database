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
#include <cctype>
#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "system/sm_meta.h"
#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"

class TransactionManager;

class QueryResultWriter {
   public:
    virtual ~QueryResultWriter() = default;

    virtual void BeginResult(const std::vector<ColMeta> &columns) = 0;
    virtual void WriteRow(const std::vector<ColMeta> &columns,
                          const std::vector<const char *> &cells) = 0;
    virtual void EndResult(rmdb::u64 row_count) = 0;
};

// used for data_send
static int const_offset = -1;

class StatementScratch {
public:
    void reset() {
        record_cursor_ = 0;
        string_cursor_ = 0;
        rid_cursor_ = 0;
    }

    std::shared_ptr<RmRecord> acquire_record(int len) {
        if (record_cursor_ == records_.size()) {
            records_.push_back(std::make_shared<RmRecord>(len));
        }
        auto record = records_[record_cursor_++];
        record->Resize(len);
        return record;
    }

    std::string *acquire_string(size_t len) {
        if (string_cursor_ == strings_.size()) {
            strings_.emplace_back();
        }
        auto *value = &strings_[string_cursor_++];
        value->resize(len);
        return value;
    }

    std::shared_ptr<std::vector<Rid>> acquire_rids() {
        if (rid_cursor_ == rid_vectors_.size()) {
            rid_vectors_.push_back(std::make_shared<std::vector<Rid>>());
        }
        auto value = rid_vectors_[rid_cursor_++];
        value->clear();
        return value;
    }

private:
    std::vector<std::shared_ptr<RmRecord>> records_;
    size_t record_cursor_ = 0;
    std::deque<std::string> strings_;
    size_t string_cursor_ = 0;
    std::vector<std::shared_ptr<std::vector<Rid>>> rid_vectors_;
    size_t rid_cursor_ = 0;
};

class Context {
public:
    Context (LockManager *lock_mgr, LogManager *log_mgr,
            TransactionManager *txn_mgr, Transaction *txn, char *data_send = nullptr, int *offset = &const_offset,
            IsolationLevel *session_isolation = nullptr, StatementScratch *scratch = nullptr,
            QueryResultWriter *result_writer = nullptr)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_mgr_(txn_mgr), txn_(txn),
          data_send_(data_send), offset_(offset), session_isolation_(session_isolation), scratch_(scratch),
          result_writer_(result_writer) {
            ellipsis_ = false;
          }

    std::shared_ptr<RmRecord> acquire_template_raw_record(int len) {
        if (scratch_ == nullptr) {
            return std::make_shared<RmRecord>(len);
        }
        return scratch_->acquire_record(len);
    }

    std::string *acquire_statement_string(size_t len) {
        if (scratch_ == nullptr) {
            return nullptr;
        }
        return scratch_->acquire_string(len);
    }

    std::shared_ptr<std::vector<Rid>> acquire_statement_rids() {
        return scratch_ == nullptr ? std::make_shared<std::vector<Rid>>() : scratch_->acquire_rids();
    }

    const std::vector<lock_data_key_t> &pending_write_intents() const {
        return pending_write_intents_view_ == nullptr
                   ? pending_write_intents_
                   : *pending_write_intents_view_;
    }

    void set_pending_write_intents_view(
        const std::vector<lock_data_key_t> *intents,
        bool canonical = false) {
        pending_write_intents_view_ = intents;
        pending_write_intents_canonical_ = canonical;
    }

    bool pending_write_intents_canonical() const {
        return pending_write_intents_canonical_;
    }

    void clear_pending_write_intents() {
        pending_write_intents_.clear();
        pending_write_intents_view_ = nullptr;
        pending_write_intents_canonical_ = false;
    }

    void set_execution_deadline(std::chrono::steady_clock::time_point deadline) {
        execution_deadline_ = deadline;
        deadline_check_counter_ = 0;
    }

    bool execution_deadline_exceeded() const {
        return execution_deadline_ != std::chrono::steady_clock::time_point::max() &&
               std::chrono::steady_clock::now() >= execution_deadline_;
    }

    void check_execution_deadline() const {
        if (execution_deadline_exceeded()) {
            const txn_id_t txn_id = txn_ == nullptr ? INVALID_TXN_ID : txn_->get_transaction_id();
            throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION,
                                            AbortSubReason::EXECUTION_BUDGET_EXCEEDED);
        }
    }

    void check_execution_deadline_periodic() {
        if ((++deadline_check_counter_ & 0xffU) == 0) {
            check_execution_deadline();
        }
    }

    LockManager *lock_mgr_;
    LogManager *log_mgr_;
    TransactionManager *txn_mgr_;
    Transaction *txn_;
    char *data_send_;
    int *offset_;
    IsolationLevel *session_isolation_;
    StatementScratch *scratch_;
    QueryResultWriter *result_writer_;
    bool ellipsis_;
    // Logical write-admission keys collected before the first statement fixes a snapshot.
    std::vector<lock_data_key_t> pending_write_intents_;
    const std::vector<lock_data_key_t> *pending_write_intents_view_{nullptr};
    bool pending_write_intents_canonical_{false};
    std::chrono::steady_clock::time_point execution_deadline_{
        std::chrono::steady_clock::time_point::max()};
    rmdb::u32 deadline_check_counter_{0};
};

namespace rmdb {

enum class TcpFrameStatus {
    kCommand,
    kNeedMore,
    kTooLarge,
};

class TcpCommandFramer {
   public:
    explicit TcpCommandFramer(size_t max_command_bytes) : max_command_bytes_(max_command_bytes) {}

    void Append(const char *data, size_t size) { pending_.append(data, size); }

    TcpFrameStatus Next(std::string *command) {
        if (command == nullptr) {
            return TcpFrameStatus::kTooLarge;
        }
        command->clear();

        while (true) {
            while (!pending_.empty() && pending_.front() == '\0') {
                pending_.erase(0, 1);
            }
            if (pending_.empty()) {
                return TcpFrameStatus::kNeedMore;
            }

            size_t nul_end = pending_.find('\0');
            size_t semicolon_end = FindStatementSemicolon(pending_, nul_end);
            if (semicolon_end != std::string::npos &&
                (nul_end == std::string::npos || semicolon_end < nul_end)) {
                size_t command_size = semicolon_end + 1;
                if (command_size > max_command_bytes_) {
                    return TcpFrameStatus::kTooLarge;
                }
                command->assign(pending_.data(), command_size);
                pending_.erase(0, command_size);
            } else if (nul_end != std::string::npos) {
                if (nul_end > max_command_bytes_) {
                    return TcpFrameStatus::kTooLarge;
                }
                command->assign(pending_.data(), nul_end);
                pending_.erase(0, nul_end + 1);
            } else {
                return pending_.size() > max_command_bytes_ ? TcpFrameStatus::kTooLarge
                                                            : TcpFrameStatus::kNeedMore;
            }

            if (!OnlyAsciiWhitespace(*command)) {
                return TcpFrameStatus::kCommand;
            }
            command->clear();
        }
    }

    size_t pending_size() const { return pending_.size(); }

   private:
    static size_t FindStatementSemicolon(const std::string &input, size_t scan_end) {
        bool in_string = false;
        size_t end = std::min(scan_end, input.size());
        for (size_t i = 0; i < end; ++i) {
            if (input[i] == '\'') {
                if (in_string && i + 1 < end && input[i + 1] == '\'') {
                    ++i;
                    continue;
                }
                in_string = !in_string;
            } else if (input[i] == ';' && !in_string) {
                return i;
            }
        }
        return std::string::npos;
    }

    static bool OnlyAsciiWhitespace(std::string_view input) {
        return std::all_of(input.begin(), input.end(), [](unsigned char ch) {
            return std::isspace(ch) != 0;
        });
    }

    size_t max_command_bytes_;
    std::string pending_;
};

}  // namespace rmdb
