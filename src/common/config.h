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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

#include "defs.h"

#define BUFFER_LENGTH 8192

/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise. */
extern std::atomic<bool> enable_logging;

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
extern std::chrono::duration<rmdb::i64> log_timeout;

using frame_id_t = rmdb::i32;  // frame id type, 帧页ID, 页在BufferPool中的存储单元称为帧,一帧对应一页
using page_id_t = rmdb::i32;   // page id type , 页ID
using txn_id_t = rmdb::i64;    // transaction id type
using lsn_t = rmdb::i64;       // log sequence number type
using slot_offset_t = rmdb::usize;  // slot offset type
using oid_t = rmdb::u16;
using timestamp_t = rmdb::i64;  // timestamp type, used for transaction concurrency

template <typename Id>
inline constexpr Id invalid_identifier = static_cast<Id>(-1);

inline constexpr frame_id_t INVALID_FRAME_ID = invalid_identifier<frame_id_t>;
inline constexpr page_id_t INVALID_PAGE_ID = invalid_identifier<page_id_t>;
inline constexpr txn_id_t INVALID_TXN_ID = invalid_identifier<txn_id_t>;
inline constexpr timestamp_t INVALID_TIMESTAMP = invalid_identifier<timestamp_t>;
inline constexpr lsn_t INVALID_LSN = invalid_identifier<lsn_t>;
inline constexpr txn_id_t TXN_START_ID = txn_id_t{1} << 62;  // first txn id
inline constexpr timestamp_t INVALID_TS = invalid_identifier<timestamp_t>;
inline constexpr page_id_t HEADER_PAGE_ID = 0;
inline constexpr rmdb::i32 PAGE_SIZE = 8 * 1024;
inline constexpr rmdb::i32 BUFFER_POOL_SIZE = 256 * 1024;
inline constexpr rmdb::i32 LOG_BUFFER_SIZE = 1024 * PAGE_SIZE;
inline constexpr rmdb::i32 BUCKET_SIZE = 50;  // size of extendible hash bucket

inline const std::string LOG_FILE_NAME{"db.log"};
inline const std::string CHECKPOINT_FILE_NAME{"db.chkpt"};
inline const std::string DB_CLEAN_SHUTDOWN_MARKER{"db.clean"};
inline const std::string DB_META_NAME{"db.meta"};
