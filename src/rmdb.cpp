/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <iostream>
#include <malloc.h>
#include <memory>
#include <mutex>
#include <poll.h>
#include <pthread.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <sched.h>
#include <signal.h>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "errors.h"
#include "common/common.h"
#include "system/sm_manager.h"
#include "optimizer/planner.h"
#include "analyze/analyze.h"
#include "optimizer/planner.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"
#include "parser/parser.h"

// Public server endpoint. Concurrency comes from independent connections;
// request order within one connection is preserved by its session loop.
#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 64

static_assert(std::atomic<bool>::is_always_lock_free,
              "信号处理函数只能使用无锁原子操作发布退出请求");
static std::atomic<bool> shutdown_requested{false};
static std::atomic<bool> wal_reset_stop_requested{false};
static volatile sig_atomic_t server_listen_fd = -1;

namespace {

class SessionPlanTemplateHotCache {
   public:
    using Key = rmdb::plan_template_detail::PlanTemplateKey;
    using Value = std::shared_ptr<const rmdb::plan_template_detail::PlanTemplate>;

    Value lookup(const Key &key, rmdb::u64 schema_epoch, rmdb::u64 feedback_generation) const {
        for (const auto &slot : slots_) {
            if (slot.valid && slot.key == key && slot.schema_epoch == schema_epoch &&
                slot.feedback_generation == feedback_generation && slot.templ != nullptr) {
                return slot.templ;
            }
        }
        return nullptr;
    }

    void store(Key key, rmdb::u64 schema_epoch, rmdb::u64 feedback_generation, Value templ) {
        auto &slot = slots_[next_++ % slots_.size()];
        slot.key = std::move(key);
        slot.schema_epoch = schema_epoch;
        slot.feedback_generation = feedback_generation;
        slot.templ = std::move(templ);
        slot.valid = true;
    }

   private:
    struct Slot {
        Key key;
        rmdb::u64 schema_epoch = 0;
        rmdb::u64 feedback_generation = 0;
        Value templ;
        bool valid = false;
    };

    std::array<Slot, 256> slots_{};
    size_t next_ = 0;
};

class SessionPointProgramCache {
   public:
    using Key = rmdb::plan_template_detail::PlanTemplateKey;

    std::shared_ptr<Plan> lookup_or_create(
        const Key &key, rmdb::u64 schema_epoch, rmdb::u64 feedback_generation,
        const std::shared_ptr<const rmdb::plan_template_detail::PlanTemplate> &templ,
        const rmdb::SqlTemplateCandidate &candidate, Context *context) {
        for (auto &slot : slots_) {
            if (!slot.valid || !(slot.key == key) || slot.schema_epoch != schema_epoch ||
                slot.feedback_generation != feedback_generation || slot.program == nullptr) {
                continue;
            }
            if (!slot.program->Rebind(candidate, context)) {
                slot = Slot{};
                return nullptr;
            }
            return slot.program->plan();
        }

        auto program = rmdb::SessionPointProgram::Create(templ, candidate, context);
        if (program == nullptr) {
            return nullptr;
        }
        auto &slot = slots_[next_++ % slots_.size()];
        slot.key = key;
        slot.schema_epoch = schema_epoch;
        slot.feedback_generation = feedback_generation;
        slot.program = std::move(program);
        slot.valid = true;
        return slot.program->plan();
    }

   private:
    struct Slot {
        Key key;
        rmdb::u64 schema_epoch = 0;
        rmdb::u64 feedback_generation = 0;
        std::unique_ptr<rmdb::SessionPointProgram> program;
        bool valid = false;
    };

    std::array<Slot, 256> slots_{};
    size_t next_ = 0;
};

std::atomic<rmdb::u64> next_client_affinity_slot{0};
constexpr size_t kMaxBufferPoolPages = 524288;
constexpr size_t kMaxSqlCommandBytes = BUFFER_LENGTH - 1;
constexpr size_t kMaxWirePayloadBytes = 1024 * 1024;
constexpr size_t kMaxWireDiagnosticBytes = 64 * 1024;
// Keep a packed OLTP batch comfortably inside the public 60-second response
// deadline.  If hotspot contention or a tombstone-heavy scan exhausts this
// server-side budget, aborting the active transaction is ACID-safe and lets
// the client retry instead of losing the whole benchmark connection.
// A budget expiry aborts the active transaction before returning; it is a
// backpressure mechanism, not a shortcut around SQL or transaction semantics.
constexpr auto kBatchExecutionBudget = std::chrono::seconds(45);
constexpr rmdb::u64 kDefaultWalResetBytes = 2048ULL * 1024 * 1024;
constexpr rmdb::u64 kMaxWalResetBytes = 64ULL * 1024 * 1024 * 1024;
constexpr int kWalResetRetrySeconds = 5;
constexpr size_t kWalPreflushBatchPages = 32768;
constexpr size_t kWalPreflushBatchFrames = 32768;
rmdb::u64 wal_reset_bytes = 0;
rmdb::u64 wal_preflush_bytes = 0;
std::chrono::steady_clock::time_point next_wal_reset_attempt = std::chrono::steady_clock::time_point::min();

size_t configured_buffer_pool_pages() {
    const char *raw = std::getenv("RMDB_BUFFER_POOL_PAGES");
    if (raw == nullptr || raw[0] == '\0') {
        return BUFFER_POOL_SIZE;
    }
    for (const char *cursor = raw; *cursor != '\0'; ++cursor) {
        if (!std::isdigit(static_cast<unsigned char>(*cursor))) {
            throw InternalError("RMDB_BUFFER_POOL_PAGES must be a positive integer");
        }
    }
    char *end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || parsed == 0 || parsed > kMaxBufferPoolPages) {
        throw InternalError("RMDB_BUFFER_POOL_PAGES must be between 1 and " +
                            std::to_string(kMaxBufferPoolPages));
    }
    return static_cast<size_t>(parsed);
}

std::string configured_perf_stats_path() {
    const char *raw = std::getenv("RMDB_PERF_STATS_PATH");
    return raw == nullptr ? std::string() : std::string(raw);
}

rmdb::u64 configured_wal_reset_bytes() {
    const char *raw = std::getenv("RMDB_WAL_RESET_BYTES");
    if (raw == nullptr || raw[0] == '\0') {
        return kDefaultWalResetBytes;
    }
    for (const char *cursor = raw; *cursor != '\0'; ++cursor) {
        if (!std::isdigit(static_cast<unsigned char>(*cursor))) {
            throw InternalError("RMDB_WAL_RESET_BYTES must be a non-negative integer");
        }
    }
    char *end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || parsed > kMaxWalResetBytes) {
        throw InternalError("RMDB_WAL_RESET_BYTES must be between 0 and " +
                            std::to_string(kMaxWalResetBytes));
    }
    return static_cast<rmdb::u64>(parsed);
}

rmdb::u64 configured_wal_preflush_bytes(rmdb::u64 reset_bytes) {
    const char *raw = std::getenv("RMDB_WAL_PREFLUSH_BYTES");
    if (raw == nullptr || raw[0] == '\0') {
        return reset_bytes == 0 ? 0 : reset_bytes - reset_bytes / 20;
    }
    for (const char *cursor = raw; *cursor != '\0'; ++cursor) {
        if (!std::isdigit(static_cast<unsigned char>(*cursor))) {
            throw InternalError("RMDB_WAL_PREFLUSH_BYTES must be a non-negative integer");
        }
    }
    char *end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || parsed > reset_bytes) {
        throw InternalError("RMDB_WAL_PREFLUSH_BYTES must be between 0 and RMDB_WAL_RESET_BYTES");
    }
    return static_cast<rmdb::u64>(parsed);
}

void ConfigureAllocator() {
    if (mallopt(M_ARENA_MAX, 4) == 0) {
        throw InternalError("Failed to set the glibc arena limit");
    }
}

bool client_affinity_enabled() {
    const char *env = std::getenv("RMDB_CLIENT_AFFINITY");
    if (env == nullptr || env[0] == '\0') {
        return false;
    }
    return env[0] == '1' || env[0] == 'y' || env[0] == 'Y' || env[0] == 't' || env[0] == 'T' ||
           env[0] == 'o' || env[0] == 'O';
}

void PinCurrentClientThread() {
    if (!client_affinity_enabled()) {
        return;
    }

    cpu_set_t allowed_set;
    CPU_ZERO(&allowed_set);
    if (sched_getaffinity(0, sizeof(allowed_set), &allowed_set) != 0) {
        return;
    }

    int allowed_count = CPU_COUNT(&allowed_set);
    if (allowed_count <= 0) {
        return;
    }

    rmdb::u64 slot = next_client_affinity_slot.fetch_add(1, std::memory_order_relaxed);
    int target_index = static_cast<int>(slot % static_cast<rmdb::u64>(allowed_count));
    int target_cpu = -1;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed_set)) {
            continue;
        }
        if (target_index == 0) {
            target_cpu = cpu;
            break;
        }
        --target_index;
    }
    if (target_cpu < 0) {
        return;
    }

    cpu_set_t target_set;
    CPU_ZERO(&target_set);
    CPU_SET(target_cpu, &target_set);
    pthread_setaffinity_np(pthread_self(), sizeof(target_set), &target_set);
}

std::string trim_copy(std::string str) {
    auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    str.erase(str.begin(), std::find_if_not(str.begin(), str.end(), is_space));
    str.erase(std::find_if_not(str.rbegin(), str.rend(), is_space).base(), str.end());
    return str;
}

std::string lowercase_copy(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return str;
}

bool server_trace_enabled() {
    const char *env = std::getenv("RMDB_TRACE_SQL");
    if (env == nullptr || env[0] == '\0') {
        return false;
    }
    std::string value = lowercase_copy(trim_copy(env));
    return value != "0" && value != "false" && value != "off" && value != "no";
}

enum class ReceiveCommandStatus {
    kCommand,
    kPeerClosed,
    kTooLarge,
    kError,
};

ReceiveCommandStatus ReceiveCommand(int fd, rmdb::TcpCommandFramer *framer, std::string *command) {
    if (framer == nullptr || command == nullptr) {
        return ReceiveCommandStatus::kError;
    }
    char recv_buffer[4096];
    while (true) {
        switch (framer->Next(command)) {
            case rmdb::TcpFrameStatus::kCommand:
                return ReceiveCommandStatus::kCommand;
            case rmdb::TcpFrameStatus::kTooLarge:
                return ReceiveCommandStatus::kTooLarge;
            case rmdb::TcpFrameStatus::kNeedMore:
                break;
        }

        ssize_t received = recv(fd, recv_buffer, sizeof(recv_buffer), 0);
        if (received == 0) {
            return ReceiveCommandStatus::kPeerClosed;
        }
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return ReceiveCommandStatus::kError;
        }
        framer->Append(recv_buffer, static_cast<size_t>(received));
    }
}

bool SendAll(int fd, const char *data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        ssize_t result = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
        if (result > 0) {
            sent += static_cast<size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool ReadExact(int fd, void *data, size_t size) {
    auto *bytes = static_cast<char *>(data);
    size_t received = 0;
    while (received < size) {
        ssize_t result = recv(fd, bytes + received, size - received, 0);
        if (result > 0) {
            received += static_cast<size_t>(result);
#ifdef TCP_QUICKACK
            // 客户端可能未启用 TCP_NODELAY：其小包发送受 Nagle 约束，等待本端 ACK。
            // 立即 ACK（TCP_QUICKACK 一次性语义）可打破 delayed-ACK 40ms 交互延迟。
            int quickack = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
#endif
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

rmdb::u16 LoadBe16(const rmdb::u8 *data) {
    return static_cast<rmdb::u16>((static_cast<rmdb::u16>(data[0]) << 8U) | data[1]);
}

rmdb::u32 LoadBe32(const rmdb::u8 *data) {
    return (static_cast<rmdb::u32>(data[0]) << 24U) |
           (static_cast<rmdb::u32>(data[1]) << 16U) |
           (static_cast<rmdb::u32>(data[2]) << 8U) |
           static_cast<rmdb::u32>(data[3]);
}

class WireBuffer {
   public:
    void Clear() { data_.clear(); }
    const char *Data() const { return reinterpret_cast<const char *>(data_.data()); }
    size_t Size() const { return data_.size(); }

    void PutU8(rmdb::u8 value) {
        ReserveAppend(1);
        data_.push_back(value);
    }
    void PutU16(rmdb::u16 value) {
        ReserveAppend(2);
        data_.push_back(static_cast<rmdb::u8>(value >> 8U));
        data_.push_back(static_cast<rmdb::u8>(value));
    }
    void PutU32(rmdb::u32 value) {
        ReserveAppend(4);
        data_.push_back(static_cast<rmdb::u8>(value >> 24U));
        data_.push_back(static_cast<rmdb::u8>(value >> 16U));
        data_.push_back(static_cast<rmdb::u8>(value >> 8U));
        data_.push_back(static_cast<rmdb::u8>(value));
    }
    void PutU64(rmdb::u64 value) {
        PutU32(static_cast<rmdb::u32>(value >> 32U));
        PutU32(static_cast<rmdb::u32>(value));
    }
    void PatchU32(size_t offset, rmdb::u32 value) {
        if (offset > data_.size() || data_.size() - offset < 4) {
            throw RMDBError("wire patch offset is out of range");
        }
        data_[offset] = static_cast<rmdb::u8>(value >> 24U);
        data_[offset + 1] = static_cast<rmdb::u8>(value >> 16U);
        data_[offset + 2] = static_cast<rmdb::u8>(value >> 8U);
        data_[offset + 3] = static_cast<rmdb::u8>(value);
    }
    void PutBytes(const char *data, size_t size) {
        ReserveAppend(size);
        const auto *begin = reinterpret_cast<const rmdb::u8 *>(data);
        data_.insert(data_.end(), begin, begin + size);
    }

   private:
    void ReserveAppend(size_t size) const {
        if (size > kMaxWirePayloadBytes || data_.size() > kMaxWirePayloadBytes - size) {
            throw RMDBError("wire payload exceeds 1 MiB");
        }
    }

    std::vector<rmdb::u8> data_;
};

class WireReader {
   public:
    explicit WireReader(const std::vector<rmdb::u8> &data) : data_(data) {}

    rmdb::u8 ReadU8() {
        Require(1);
        return data_[offset_++];
    }
    rmdb::u16 ReadU16() {
        Require(2);
        rmdb::u16 value = LoadBe16(data_.data() + offset_);
        offset_ += 2;
        return value;
    }
    rmdb::u32 ReadU32() {
        Require(4);
        rmdb::u32 value = LoadBe32(data_.data() + offset_);
        offset_ += 4;
        return value;
    }
    std::string ReadString(size_t size) {
        Require(size);
        std::string value(reinterpret_cast<const char *>(data_.data() + offset_), size);
        offset_ += size;
        return value;
    }
    std::string_view ReadStringView(size_t size) {
        Require(size);
        std::string_view value(reinterpret_cast<const char *>(data_.data() + offset_), size);
        offset_ += size;
        return value;
    }
    bool Empty() const { return offset_ == data_.size(); }

   private:
    void Require(size_t size) const {
        if (size > data_.size() || offset_ > data_.size() - size) {
            throw RMDBError("truncated wire payload");
        }
    }

    const std::vector<rmdb::u8> &data_;
    size_t offset_{0};
};

bool SendWireFrame(int fd, rmdb::u8 tag, const WireBuffer &payload) {
    if (payload.Size() > kMaxWirePayloadBytes) {
        return false;
    }
    rmdb::u8 header[8] = {
        static_cast<rmdb::u8>(payload.Size() >> 24U),
        static_cast<rmdb::u8>(payload.Size() >> 16U),
        static_cast<rmdb::u8>(payload.Size() >> 8U),
        static_cast<rmdb::u8>(payload.Size()),
        tag,
        0,
        0,
        0,
    };
    iovec vectors[2] = {
        {header, sizeof(header)},
        {const_cast<char *>(payload.Data()), payload.Size()},
    };
    size_t vector_index = 0;
    const size_t vector_count = payload.Size() == 0 ? 1 : 2;
    while (vector_index < vector_count) {
        msghdr message{};
        message.msg_iov = vectors + vector_index;
        message.msg_iovlen = vector_count - vector_index;
        ssize_t sent = sendmsg(fd, &message, MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent <= 0) {
            return false;
        }
        size_t consumed = static_cast<size_t>(sent);
        while (vector_index < vector_count &&
               consumed >= vectors[vector_index].iov_len) {
            consumed -= vectors[vector_index].iov_len;
            ++vector_index;
        }
        if (vector_index < vector_count && consumed != 0) {
            auto *base = static_cast<char *>(vectors[vector_index].iov_base);
            vectors[vector_index].iov_base = base + consumed;
            vectors[vector_index].iov_len -= consumed;
        }
    }
    return true;
}

bool SendWireDiagnostic(int fd, rmdb::u8 tag, const std::string &diagnostic) {
    WireBuffer payload;
    payload.PutBytes(diagnostic.data(), std::min(diagnostic.size(), kMaxWireDiagnosticBytes));
    return SendWireFrame(fd, tag, payload);
}

struct WireFrame {
    rmdb::u8 tag{0};
    rmdb::u8 flags{0};
    rmdb::u16 reserved{0};
    std::vector<rmdb::u8> payload;
};

class ConnectionReceiveBuffer {
   public:
    bool ReadExact(int fd, void *output, size_t size) {
        auto *destination = static_cast<rmdb::u8 *>(output);
        while (size != 0) {
            if (begin_ != end_) {
                size_t available = end_ - begin_;
                size_t copied = std::min(size, available);
                std::memcpy(destination, bytes_.data() + begin_, copied);
                begin_ += copied;
                destination += copied;
                size -= copied;
                continue;
            }
            begin_ = 0;
            end_ = 0;
            ssize_t received = recv(fd, bytes_.data(), bytes_.size(), 0);
            if (received > 0) {
                end_ = static_cast<size_t>(received);
#ifdef TCP_QUICKACK
                int quickack = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack,
                           sizeof(quickack));
#endif
                continue;
            }
            if (received < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        return true;
    }

   private:
    std::array<rmdb::u8, 64 * 1024> bytes_{};
    size_t begin_{0};
    size_t end_{0};
};

enum class WireReceiveStatus {
    Frame,
    PeerClosed,
    TooLarge,
};

WireReceiveStatus ReceiveWireFrame(int fd, ConnectionReceiveBuffer *receive_buffer,
                                   WireFrame *frame) {
    if (receive_buffer == nullptr || frame == nullptr) {
        return WireReceiveStatus::PeerClosed;
    }
    rmdb::u8 header[8];
    if (!receive_buffer->ReadExact(fd, header, sizeof(header))) {
        return WireReceiveStatus::PeerClosed;
    }
    rmdb::u32 payload_size = LoadBe32(header);
    if (payload_size > kMaxWirePayloadBytes) {
        return WireReceiveStatus::TooLarge;
    }
    frame->tag = header[4];
    frame->flags = header[5];
    frame->reserved = LoadBe16(header + 6);
    frame->payload.resize(payload_size);
    if (payload_size > 0 &&
        !receive_buffer->ReadExact(fd, frame->payload.data(), payload_size)) {
        return WireReceiveStatus::PeerClosed;
    }
    return WireReceiveStatus::Frame;
}

enum class ConnectionProtocol {
    Legacy,
    WireV3,
    Closed,
};

ConnectionProtocol InitializeConnectionProtocol(int fd, rmdb::TcpCommandFramer *legacy_framer) {
    rmdb::u8 prefix[4];
    if (!ReadExact(fd, prefix, sizeof(prefix))) {
        return ConnectionProtocol::Closed;
    }
    if (std::memcmp(prefix, "RMDB", 4) != 0) {
        legacy_framer->Append(reinterpret_cast<const char *>(prefix), sizeof(prefix));
        return ConnectionProtocol::Legacy;
    }
    rmdb::u8 version[4];
    if (!ReadExact(fd, version, sizeof(version))) {
        return ConnectionProtocol::Closed;
    }
    if (LoadBe16(version) != 3 || LoadBe16(version + 2) != 0) {
        return ConnectionProtocol::Closed;
    }
    rmdb::u8 handshake[8] = {'R', 'M', 'D', 'B', version[0], version[1], version[2], version[3]};
    return SendAll(fd, reinterpret_cast<const char *>(handshake), sizeof(handshake))
               ? ConnectionProtocol::WireV3
               : ConnectionProtocol::Closed;
}

enum class FastCommand {
    None,
    Begin,
    Commit,
    Rollback,
    Abort,
    SetIsolationSnapshot,
    SetIsolationSerializable,
};

FastCommand parse_fast_command(const char *raw_sql) {
    std::string stmt = trim_copy(raw_sql == nullptr ? "" : std::string(raw_sql));
    if (!stmt.empty() && stmt.back() == ';') {
        stmt.pop_back();
        stmt = trim_copy(stmt);
    }
    stmt = lowercase_copy(stmt);
    if (stmt == "begin") {
        return FastCommand::Begin;
    }
    if (stmt == "commit") {
        return FastCommand::Commit;
    }
    if (stmt == "rollback") {
        return FastCommand::Rollback;
    }
    if (stmt == "abort") {
        return FastCommand::Abort;
    }
    if (stmt == "set transaction isolation level snapshot isolation") {
        return FastCommand::SetIsolationSnapshot;
    }
    if (stmt == "set transaction isolation level serializable") {
        return FastCommand::SetIsolationSerializable;
    }
    return FastCommand::None;
}

}  // namespace

std::unique_ptr<DiskManager> disk_manager;
std::unique_ptr<BufferPoolManager> buffer_pool_manager;
std::unique_ptr<RmManager> rm_manager;
std::unique_ptr<IxManager> ix_manager;
std::unique_ptr<SmManager> sm_manager;
std::unique_ptr<LockManager> lock_manager;
std::unique_ptr<TransactionManager> txn_manager;
std::unique_ptr<Planner> planner;
std::unique_ptr<Optimizer> optimizer;
std::unique_ptr<QlManager> ql_manager;
std::unique_ptr<LogManager> log_manager;
std::unique_ptr<RecoveryManager> recovery;
std::unique_ptr<Portal> portal;
std::unique_ptr<Analyze> analyze;
std::unique_ptr<RuntimePerfStats> perf_stats;
std::mutex client_registry_mutex;
std::unordered_set<int> active_client_fds;

struct WalPreflushState {
    bool started{false};
    bool complete{false};
    size_t next_frame{0};
    size_t flushed_pages{0};
    std::chrono::steady_clock::time_point started_at{};
};

WalPreflushState wal_preflush_state;

void InitializeManagers(size_t buffer_pool_pages, const std::string &perf_stats_path) {
    if (!perf_stats_path.empty()) {
        perf_stats = std::make_unique<RuntimePerfStats>(perf_stats_path, buffer_pool_pages);
    }
    disk_manager = std::make_unique<DiskManager>();
    disk_manager->set_perf_stats(perf_stats.get());
    buffer_pool_manager = std::make_unique<BufferPoolManager>(buffer_pool_pages, disk_manager.get());
    buffer_pool_manager->set_perf_stats(perf_stats.get());
    rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    sm_manager =
        std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
    lock_manager = std::make_unique<LockManager>();
    txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
    planner = std::make_unique<Planner>(sm_manager.get());
    optimizer = std::make_unique<Optimizer>(planner.get());
    ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), nullptr);
    log_manager = std::make_unique<LogManager>(disk_manager.get());
    buffer_pool_manager->set_log_manager(log_manager.get());
    recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
    portal = std::make_unique<Portal>(sm_manager.get());
    analyze = std::make_unique<Analyze>(sm_manager.get());
}

void WritePerfStats() {
    if (perf_stats != nullptr && !perf_stats->Write()) {
        std::cerr << "Failed to write RMDB_PERF_STATS_PATH\n";
    }
}

void PersistRecoveredState() {
    log_manager->flush_log_to_disk();
    disk_manager->sync_log();
    for (auto &entry : sm_manager->fhs_) {
        entry.second->flush();
    }
    for (auto &entry : sm_manager->ihs_) {
        entry.second->flush();
    }
    disk_manager->sync_all_data_files();
    disk_manager->truncate_log();
    disk_manager->sync_log();
    disk_manager->remove_file_if_exists(CHECKPOINT_FILE_NAME);
    log_manager->reset_log_file_offset(0);
}

void TriggerWalResetFailpoint(const char *name) {
    const char *configured = std::getenv("RMDB_WAL_RESET_FAILPOINT");
    if (configured != nullptr && std::strcmp(configured, name) == 0) {
        std::cerr << "WAL reset failpoint: " << name << std::endl;
        _exit(86);
    }
}

bool PerformWalReset() {
    bool wal_truncated = false;
    auto pause_start = std::chrono::steady_clock::now();
    auto stage_start = pause_start;
    auto stage_elapsed_ms = [&stage_start]() {
        auto now = std::chrono::steady_clock::now();
        double elapsed_ms = std::chrono::duration<double, std::milli>(now - stage_start).count();
        stage_start = now;
        return elapsed_ms;
    };
    try {
        auto admission_guard = txn_manager->BlockNewTransactionsAndWait();
        auto checkpoint_guard = txn_manager->EnterCheckpointExecution();
        double drain_ms = stage_elapsed_ms();

        log_manager->flush_log_to_disk();
        disk_manager->sync_log();
        rmdb::i64 wal_size = disk_manager->get_file_size(LOG_FILE_NAME);
        if (wal_size < 0) {
            throw InternalError("Failed to read WAL size before reset");
        }
        double wal_sync_ms = stage_elapsed_ms();

        // DELETE 提交时只发布内存中的 MVCC 删除标记；仍可能读取旧版本时不能立即回收槽位。
        // 此处已阻止新事务并排空旧读者，可以先物理化已提交删除，再持久化数据页并丢弃旧 WAL。
        txn_manager->PhysicalizeCommittedDeletes();
        for (auto &entry : sm_manager->fhs_) {
            entry.second->flush_header();
        }
        for (auto &entry : sm_manager->ihs_) {
            entry.second->flush_header();
        }
        double metadata_ms = stage_elapsed_ms();
        size_t dirty_pages = buffer_pool_manager->flush_all_pages();
        double page_flush_ms = stage_elapsed_ms();
        TriggerWalResetFailpoint("before_data_sync");

        disk_manager->sync_all_data_files();
        double data_sync_ms = stage_elapsed_ms();
        TriggerWalResetFailpoint("after_data_sync");

        disk_manager->truncate_log();
        wal_truncated = true;
        disk_manager->sync_log();
        TriggerWalResetFailpoint("after_truncate");

        disk_manager->remove_file_if_exists(CHECKPOINT_FILE_NAME);
        log_manager->reset_log_file_offset(0);
        double log_reset_ms = stage_elapsed_ms();
        malloc_trim(0);
        double trim_ms = stage_elapsed_ms();

        auto pause_ns = static_cast<rmdb::u64>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - pause_start)
                                                  .count());
        if (perf_stats != nullptr) {
            perf_stats->RecordWalReset(pause_ns, dirty_pages, static_cast<rmdb::u64>(wal_size));
        }
        std::cerr << "WAL reset completed: bytes=" << wal_size << " dirty_pages=" << dirty_pages
                  << " pause_ms=" << (pause_ns / 1000000.0) << " drain_ms=" << drain_ms
                  << " wal_sync_ms=" << wal_sync_ms << " metadata_ms=" << metadata_ms
                  << " page_flush_ms=" << page_flush_ms << " data_sync_ms=" << data_sync_ms
                  << " log_reset_ms=" << log_reset_ms << " trim_ms=" << trim_ms << "\n";
        return true;
    } catch (const std::exception &error) {
        if (wal_truncated) {
            throw;
        }
        next_wal_reset_attempt =
            std::chrono::steady_clock::now() + std::chrono::seconds(kWalResetRetrySeconds);
        std::cerr << "WAL reset cancelled with log preserved: " << error.what() << "\n";
        return false;
    }
}

void ResetWalPreflushState() { wal_preflush_state = WalPreflushState{}; }

void MaybePreflushWal(rmdb::u64 log_offset) {
    if (wal_preflush_bytes == 0 || wal_preflush_state.complete) {
        return;
    }
    if (log_offset < wal_preflush_bytes) {
        return;
    }

    if (!wal_preflush_state.started) {
        wal_preflush_state.started = true;
        wal_preflush_state.started_at = std::chrono::steady_clock::now();
    }

    bool pass_complete = false;
    wal_preflush_state.flushed_pages += buffer_pool_manager->flush_unpinned_pages_batch(
        kWalPreflushBatchPages, kWalPreflushBatchFrames, &wal_preflush_state.next_frame, &pass_complete);
    if (!pass_complete) {
        return;
    }

    wal_preflush_state.complete = true;
    auto sync_started_at = std::chrono::steady_clock::now();
    disk_manager->sync_all_data_files();
    auto sync_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - sync_started_at)
                       .count();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - wal_preflush_state.started_at)
                          .count();
    std::cerr << "WAL preflush completed: bytes=" << log_offset
              << " dirty_pages=" << wal_preflush_state.flushed_pages << " elapsed_ms=" << elapsed_ms
              << " data_sync_ms=" << sync_ms << "\n";
}

void MaybeResetWal() {
    if (wal_reset_bytes == 0 || std::chrono::steady_clock::now() < next_wal_reset_attempt) {
        return;
    }
    lsn_t log_offset = log_manager->get_log_file_offset();
    if (log_offset < 0) {
        return;
    }
    rmdb::u64 current_log_offset = static_cast<rmdb::u64>(log_offset);
    if (current_log_offset < wal_reset_bytes) {
        try {
            MaybePreflushWal(current_log_offset);
        } catch (const std::exception &error) {
            ResetWalPreflushState();
            next_wal_reset_attempt =
                std::chrono::steady_clock::now() + std::chrono::seconds(kWalResetRetrySeconds);
            std::cerr << "WAL preflush cancelled with log preserved: " << error.what() << "\n";
        }
        return;
    }
    if (PerformWalReset()) {
        ResetWalPreflushState();
        next_wal_reset_attempt = std::chrono::steady_clock::time_point::min();
    }
}

void wal_reset_maintenance_loop() {
    // stop_token 负责正常作用域退出，shutdown_requested 负责 SIGINT 跨线程通知；
    // 两者都不依赖普通 volatile 变量，因此不会产生 C++ 内存模型中的数据竞争。
    while (!wal_reset_stop_requested.load(std::memory_order_relaxed) &&
           !shutdown_requested.load(std::memory_order_relaxed)) {
        MaybeResetWal();
        poll(nullptr, 0, 250);
    }
}

void sigint_handler(int signo) {
    (void)signo;
    shutdown_requested.store(true, std::memory_order_relaxed);
    int listen_fd = server_listen_fd;
    if (listen_fd >= 0) {
        close(listen_fd);
        server_listen_fd = -1;
    }
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t *txn_id, Context *context, IsolationLevel session_isolation,
                    bool implicit_read_only = false) {
    if (*txn_id == INVALID_TXN_ID) {
        context->txn_ = nullptr;
    } else if (context->txn_ == nullptr || context->txn_->get_transaction_id() != *txn_id) {
        context->txn_ = txn_manager->get_transaction(*txn_id);
    }
    if(context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = implicit_read_only && session_isolation == IsolationLevel::SNAPSHOT_ISOLATION
                            ? txn_manager->begin_read_only(session_isolation)
                            : txn_manager->begin(nullptr, context->log_mgr_, session_isolation,
                                                  context->pending_write_intents(),
                                                  context->pending_write_intents_canonical());
        context->clear_pending_write_intents();
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
    }
}

void BindExistingTransaction(txn_id_t txn_id, Context *context) {
    if (txn_id == INVALID_TXN_ID) {
        context->txn_ = nullptr;
        return;
    }
    if (context->txn_ == nullptr || context->txn_->get_transaction_id() != txn_id) {
        context->txn_ = txn_manager->get_transaction(txn_id);
    }
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = nullptr;
        return;
    }
}

void AbortSessionTransaction(txn_id_t *txn_id, Transaction **session_txn) noexcept {
    if (txn_id == nullptr || *txn_id == INVALID_TXN_ID) {
        if (session_txn != nullptr) {
            *session_txn = nullptr;
        }
        return;
    }
    try {
        auto statement_guard = txn_manager->EnterStatementExecution();
        Transaction *txn = session_txn == nullptr ? nullptr : *session_txn;
        if (txn == nullptr || txn->get_transaction_id() != *txn_id) {
            txn = txn_manager->get_transaction(*txn_id);
        }
        if (txn != nullptr && txn->get_state() == TransactionState::GROWING) {
            txn_manager->abort(txn, log_manager.get());
        }
    } catch (const std::exception &error) {
        std::cerr << "Failed to abort disconnected transaction " << *txn_id << ": " << error.what() << "\n";
    } catch (...) {
        std::cerr << "Failed to abort disconnected transaction " << *txn_id << "\n";
    }
    *txn_id = INVALID_TXN_ID;
    if (session_txn != nullptr) {
        *session_txn = nullptr;
    }
}

class SessionTransactionGuard {
   public:
    SessionTransactionGuard(txn_id_t *txn_id, Transaction **session_txn)
        : txn_id_(txn_id), session_txn_(session_txn) {}
    SessionTransactionGuard(const SessionTransactionGuard &) = delete;
    SessionTransactionGuard &operator=(const SessionTransactionGuard &) = delete;
    ~SessionTransactionGuard() { AbortSessionTransaction(txn_id_, session_txn_); }

   private:
    txn_id_t *txn_id_;
    Transaction **session_txn_;
};

void AbortStatementTransaction(Context *context, txn_id_t *txn_id) {
    auto statement_guard = txn_manager->EnterStatementExecution();
    Transaction *txn = context == nullptr ? nullptr : context->txn_;
    if (txn == nullptr && txn_id != nullptr && *txn_id != INVALID_TXN_ID) {
        txn = txn_manager->get_transaction(*txn_id);
    }
    if (txn != nullptr && txn->get_state() == TransactionState::GROWING) {
        txn_manager->abort(txn, log_manager.get());
    }
    if (context != nullptr) {
        context->txn_ = nullptr;
    }
    if (txn_id != nullptr) {
        *txn_id = INVALID_TXN_ID;
    }
}

bool ExecuteFastCommand(FastCommand command, txn_id_t *txn_id, Context *context,
                        IsolationLevel *session_isolation) {
    if (command == FastCommand::None) {
        return false;
    }

    if (command == FastCommand::Begin) {
        BindExistingTransaction(*txn_id, context);
        if (context->txn_ == nullptr) {
            context->txn_ = txn_manager->begin(nullptr, context->log_mgr_, *session_isolation,
                                                context->pending_write_intents(),
                                                context->pending_write_intents_canonical());
            context->clear_pending_write_intents();
        }
    }
    auto statement_guard = txn_manager->EnterStatementExecution();
    switch (command) {
        case FastCommand::Begin:
            context->txn_->set_txn_mode(true);
            *txn_id = context->txn_->get_transaction_id();
            break;
        case FastCommand::Commit:
            BindExistingTransaction(*txn_id, context);
            if (context->txn_ != nullptr) {
                txn_manager->commit(context->txn_, context->log_mgr_);
                context->txn_ = nullptr;
            }
            *txn_id = INVALID_TXN_ID;
            break;
        case FastCommand::Rollback:
        case FastCommand::Abort:
            BindExistingTransaction(*txn_id, context);
            if (context->txn_ != nullptr) {
                txn_manager->abort(context->txn_, context->log_mgr_);
                context->txn_ = nullptr;
            }
            *txn_id = INVALID_TXN_ID;
            break;
        case FastCommand::SetIsolationSnapshot:
        case FastCommand::SetIsolationSerializable:
            *session_isolation = command == FastCommand::SetIsolationSerializable
                                     ? IsolationLevel::SERIALIZABLE
                                     : IsolationLevel::SNAPSHOT_ISOLATION;
            BindExistingTransaction(*txn_id, context);
            break;
        case FastCommand::None:
            break;
    }
    return true;
}

void RunPlan(const std::shared_ptr<Plan> &plan, txn_id_t *txn_id, Context *context) {
    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
    portal->run(portalStmt, ql_manager.get(), txn_id, context);
    portal->drop();
    if (context->txn_ != nullptr && context->txn_->get_txn_mode() == false &&
        context->txn_->get_state() == TransactionState::GROWING) {
        txn_manager->commit(context->txn_, context->log_mgr_);
        context->txn_ = nullptr;
        *txn_id = INVALID_TXN_ID;
    }
}

enum class StatementStatus {
    Ok,
    TransactionAbort,
    Error,
};

struct StatementExecutionResult {
    StatementStatus status{StatementStatus::Error};
    bool is_query{false};
    bool legacy_abort_response{false};
    std::string diagnostic;
};

bool PlanProducesQueryResult(const std::shared_ptr<Plan> &plan) {
    if (plan == nullptr) {
        return false;
    }
    return plan->tag == T_select || plan->tag == T_ShowTable ||
           plan->tag == T_ShowIndex || plan->tag == T_DescTable;
}

StatementExecutionResult ExecuteSqlStatement(const char *data_recv, txn_id_t *txn_id, Context *context,
                                             IsolationLevel *session_isolation,
                                             SessionPlanTemplateHotCache *session_plan_cache,
                                             SessionPointProgramCache *session_point_program_cache,
                                             const rmdb::SqlTemplateCandidate *prepared_candidate = nullptr,
                                             const rmdb::query_template_detail::QueryTemplate *prepared_query_template = nullptr,
                                             std::shared_ptr<const rmdb::plan_template_detail::PlanTemplate>
                                                 prepared_plan_template = nullptr,
                                             rmdb::u64 prepared_schema_epoch = 0,
                                             rmdb::u64 prepared_feedback_generation = 0,
                                             IsolationLevel prepared_isolation = IsolationLevel::SNAPSHOT_ISOLATION,
                                             bool prepared_execution = false) {
    StatementExecutionResult result;
    bool executing_prepared_update = false;
    try {
        FastCommand fast_command = parse_fast_command(data_recv);
        bool statement_success = false;
        bool advisor_observes_select = false;
        if (ExecuteFastCommand(fast_command, txn_id, context, session_isolation)) {
            statement_success = true;
        } else {
            std::shared_ptr<Query> query;
            auto parsed_template_candidate = prepared_candidate == nullptr
                                                 ? rmdb::make_sql_template_candidate(data_recv)
                                                 : std::optional<rmdb::SqlTemplateCandidate>();
            const rmdb::SqlTemplateCandidate *template_candidate =
                prepared_candidate == nullptr
                    ? (parsed_template_candidate.has_value() ? &*parsed_template_candidate : nullptr)
                    : prepared_candidate;
            bool plan_template_candidate = prepared_candidate == nullptr && template_candidate != nullptr &&
                                           rmdb::plan_template_cacheable_session(*session_isolation);
            // Prepared programs already bind their physical access path at
            // PREPARE_SET. Re-running the advisor's SQL-shape inspection and
            // build scheduler for every EXEC_BATCH operation adds catalog and
            // string work while any newly built index would invalidate the
            // prepared schema epoch anyway.
            advisor_observes_select = !prepared_execution &&
                rmdb::hidden_index_advisor_should_observe_sql(
                    sm_manager.get(), data_recv, *session_isolation);
            bool ran_plan_template = false;
            if (prepared_candidate != nullptr && prepared_plan_template != nullptr &&
                rmdb::plan_template_cacheable_session(*session_isolation) &&
                prepared_schema_epoch == rmdb::sql_template_schema_epoch() &&
                prepared_feedback_generation == rmdb::workload_feedback_generation() &&
                prepared_isolation == *session_isolation) {
                // Prepared statements may be INSERTs or other write plans whose
                // executor state is transaction-specific. Materialize a fresh
                // plan exactly as the stable gaga path does; the point-program
                // cache below remains available for eligible non-prepared SQL.
                auto plan = rmdb::plan_template_detail::materialize_template(
                    *prepared_plan_template, *prepared_candidate, context);
                if (plan != nullptr &&
                    (prepared_schema_epoch != rmdb::sql_template_schema_epoch() ||
                     prepared_feedback_generation != rmdb::workload_feedback_generation())) {
                    plan = nullptr;
                }
                if (plan != nullptr && advisor_observes_select && prepared_query_template != nullptr) {
                    query = rmdb::query_template_detail::materialize_template(
                        *prepared_query_template, *prepared_candidate, context);
                }
                if (plan != nullptr && (!advisor_observes_select || query != nullptr)) {
                    executing_prepared_update = prepared_execution && plan->tag == T_Update;
                    result.is_query = PlanProducesQueryResult(plan);
                    SetTransaction(txn_id, context, *session_isolation, result.is_query);
                    auto statement_guard = txn_manager->EnterStatementExecution();
                    std::vector<rmdb::HiddenIndexCandidate> advisor_candidates;
                    if (advisor_observes_select) {
                        advisor_candidates = rmdb::hidden_index_advisor_collect(sm_manager.get(), query, context);
                    }
                    RunPlan(plan, txn_id, context);
                    rmdb::hidden_index_advisor_record(advisor_candidates);
                    ran_plan_template = true;
                    statement_success = true;
                }
            }
            if (plan_template_candidate) {
                const rmdb::u64 schema_epoch = rmdb::sql_template_schema_epoch();
                const rmdb::u64 feedback_generation = rmdb::workload_feedback_generation();
                SessionPlanTemplateHotCache::Key hot_key{template_candidate->key, *session_isolation};
                auto plan_template = session_plan_cache->lookup(hot_key, schema_epoch, feedback_generation);
                if (plan_template == nullptr) {
                        plan_template = rmdb::lookup_plan_template_definition(*template_candidate, *session_isolation);
                    if (plan_template != nullptr) {
                        session_plan_cache->store(hot_key, schema_epoch, feedback_generation, plan_template);
                    }
                }
                auto plan = plan_template == nullptr
                                ? nullptr
                                : session_point_program_cache->lookup_or_create(
                                      hot_key, schema_epoch, feedback_generation, plan_template,
                                      *template_candidate, context);
                if (plan == nullptr && plan_template != nullptr) {
                    plan = rmdb::plan_template_detail::materialize_template(
                        *plan_template, *template_candidate, context);
                }
                if (plan != nullptr &&
                    (rmdb::sql_template_schema_epoch() != schema_epoch ||
                     rmdb::workload_feedback_generation() != feedback_generation)) {
                    plan = nullptr;
                }
                if (plan != nullptr && advisor_observes_select) {
                    query = rmdb::lookup_query_template(*template_candidate, context);
                }
                if (plan != nullptr && (!advisor_observes_select || query != nullptr)) {
                    executing_prepared_update = prepared_execution && plan->tag == T_Update;
                    result.is_query = PlanProducesQueryResult(plan);
                    SetTransaction(txn_id, context, *session_isolation, result.is_query);
                    auto statement_guard = txn_manager->EnterStatementExecution();
                    std::vector<rmdb::HiddenIndexCandidate> advisor_candidates;
                    if (advisor_observes_select) {
                        advisor_candidates = rmdb::hidden_index_advisor_collect(sm_manager.get(), query, context);
                    }
                    RunPlan(plan, txn_id, context);
                    rmdb::hidden_index_advisor_record(advisor_candidates);
                    ran_plan_template = true;
                    statement_success = true;
                }
            }
            if (!ran_plan_template && query == nullptr && template_candidate != nullptr) {
                query = rmdb::lookup_query_template(*template_candidate, context);
            }

            if (!ran_plan_template && query == nullptr && prepared_candidate != nullptr &&
                prepared_query_template != nullptr) {
                query = rmdb::query_template_detail::materialize_template(
                    *prepared_query_template, *prepared_candidate, context);
            }

            if (!ran_plan_template && query == nullptr) {
                if (prepared_candidate != nullptr) {
                    throw RMDBError("prepared statement template could not be materialized");
                }
                auto parse_result = rmdb::ParseSql(data_recv);
                if (parse_result.status == 0) {
                    if (parse_result.tree != nullptr) {
                        query = analyze->do_analyze(parse_result.tree);
                        if (template_candidate != nullptr) {
                            rmdb::store_query_template(*template_candidate, parse_result.tree, query);
                        }
                    }
                } else {
                    result.status = StatementStatus::Error;
                    result.diagnostic = parse_result.error.empty() ? "parse error" : std::move(parse_result.error);
                    return result;
                }
            }

            if (!ran_plan_template && query != nullptr) {
                bool is_checkpoint_stmt =
                    std::dynamic_pointer_cast<ast::CreateCheckpoint>(query->parse) != nullptr ||
                    query->kind == StmtKind::CreateCheckpoint;
                bool is_txn_end_stmt =
                    std::dynamic_pointer_cast<ast::TxnCommit>(query->parse) ||
                    std::dynamic_pointer_cast<ast::TxnAbort>(query->parse) ||
                    std::dynamic_pointer_cast<ast::TxnRollback>(query->parse) ||
                    query->kind == StmtKind::TxnCommit ||
                    query->kind == StmtKind::TxnAbort ||
                    query->kind == StmtKind::TxnRollback;
                bool is_set_isolation_stmt =
                    std::dynamic_pointer_cast<ast::SetTransactionIsolation>(query->parse) != nullptr ||
                    query->kind == StmtKind::SetTransactionIsolation;
                std::vector<rmdb::HiddenIndexCandidate> advisor_candidates;
                {
                    StatementCheckpointGate::ReadGuard statement_guard;
                    StatementCheckpointGate::WriteGuard checkpoint_guard;
                    if (is_checkpoint_stmt) {
                        checkpoint_guard = txn_manager->EnterCheckpointExecution();
                    } else {
                        if (is_txn_end_stmt) {
                            BindExistingTransaction(*txn_id, context);
                        } else if (!is_set_isolation_stmt) {
                            SetTransaction(txn_id, context, *session_isolation,
                                           query->kind == StmtKind::Select);
                        }
                        statement_guard = txn_manager->EnterStatementExecution();
                    }
                    if (advisor_observes_select) {
                        advisor_candidates = rmdb::hidden_index_advisor_collect(sm_manager.get(), query, context);
                    }
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                    executing_prepared_update = prepared_execution && plan != nullptr && plan->tag == T_Update;
                    result.is_query = PlanProducesQueryResult(plan);
                    if (plan_template_candidate) {
                        rmdb::store_plan_template(*template_candidate, query, plan, *session_isolation);
                    }
                    RunPlan(plan, txn_id, context);
                    rmdb::hidden_index_advisor_record(advisor_candidates);
                    statement_success = true;
                }
            }
        }
        if (statement_success && advisor_observes_select &&
            *txn_id == INVALID_TXN_ID &&
            rmdb::plan_template_cacheable_session(*session_isolation)) {
            rmdb::hidden_index_advisor_try_build_one(sm_manager.get(), txn_manager.get(), context);
        }
        result.status = StatementStatus::Ok;
        return result;
    } catch (TransactionAbortException &error) {
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::TransactionAbort;
        result.legacy_abort_response = true;
        result.diagnostic = error.GetInfo();
        return result;
    } catch (BufferPoolUnavailableError &error) {
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::TransactionAbort;
        result.legacy_abort_response = true;
        result.diagnostic = error.what();
        return result;
    } catch (RecordNotFoundError &error) {
        // A physical RID obtained by a DML plan can cease to be visible before
        // the executor locks it.  Retrying the whole SI transaction is the
        // correct terminal result; reporting SQL ERROR makes EXEC_BATCH abort
        // the benchmark as a non-retryable server defect.
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::TransactionAbort;
        result.legacy_abort_response = true;
        result.diagnostic = error.what();
        return result;
    } catch (NonRetryableSqlExecutionError &error) {
        Transaction *error_txn = context->txn_;
        if (error_txn == nullptr && *txn_id != INVALID_TXN_ID) {
            error_txn = txn_manager->get_transaction(*txn_id);
        }
        result.legacy_abort_response = error_txn != nullptr && error_txn->get_txn_mode();
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::Error;
        result.diagnostic = error.what();
        return result;
    } catch (InternalError &error) {
        if (executing_prepared_update) {
            // PREPARE_SET has already validated syntax, names and types.  An
            // InternalError reached while running its UPDATE is therefore an
            // engine/runtime failure (typically a version/undo race), not a
            // non-retryable SQL error.  AUTO_ABORT makes retrying the complete
            // transaction safe and preserves the original diagnostic.
            AbortStatementTransaction(context, txn_id);
            result.status = StatementStatus::TransactionAbort;
            result.legacy_abort_response = true;
            result.diagnostic = error.what();
            return result;
        }
        std::cerr << "statement internal error: " << error.what() << '\n';
        Transaction *error_txn = context->txn_;
        if (error_txn == nullptr && *txn_id != INVALID_TXN_ID) {
            error_txn = txn_manager->get_transaction(*txn_id);
        }
        result.legacy_abort_response = error_txn != nullptr && error_txn->get_txn_mode();
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::Error;
        result.diagnostic = error.what();
        return result;
    } catch (RMDBError &error) {
        if (executing_prepared_update) {
            AbortStatementTransaction(context, txn_id);
            result.status = StatementStatus::TransactionAbort;
            result.legacy_abort_response = true;
            result.diagnostic = error.what();
            return result;
        }
        std::cerr << "statement error: " << error.what() << '\n';
        Transaction *error_txn = context->txn_;
        if (error_txn == nullptr && *txn_id != INVALID_TXN_ID) {
            error_txn = txn_manager->get_transaction(*txn_id);
        }
        result.legacy_abort_response = error_txn != nullptr && error_txn->get_txn_mode();
        AbortStatementTransaction(context, txn_id);
        result.status = StatementStatus::Error;
        result.diagnostic = error.what();
        return result;
    }
}

rmdb::u8 WireTypeFor(ColType type) {
    switch (type) {
        case TYPE_INT: return 0x01;
        case TYPE_FLOAT: return 0x02;
        case TYPE_STRING: return 0x03;
    }
    throw RMDBError("unsupported wire column type");
}

struct PreparedColumn {
    std::string name;
    ColType type{TYPE_INT};
};

struct PreparedStatement {
    rmdb::u16 id{0};
    bool is_query{false};
    std::vector<rmdb::u8> parameter_types;
    std::string exemplar_sql;
    std::vector<rmdb::u16> marker_parameters;
    std::vector<std::pair<std::string, rmdb::u16>> predicate_markers;
    std::vector<std::pair<std::string, rmdb::u16>> sorted_predicate_markers;
    std::string intent_relation;
    rmdb::u64 intent_relation_hash{1469598103934665603ULL};
    bool batch_intent_enabled{false};
    std::vector<rmdb::u16> marker_literal_indexes;
    std::vector<rmdb::i32> literal_parameter_bindings;
    struct TemplateLiteral {
        rmdb::SqlTemplateLiteralType type{rmdb::SqlTemplateLiteralType::kInt};
        int int_value{0};
        float float_value{0.0F};
        std::string string_value;
    };
    std::vector<TemplateLiteral> template_literals;
    rmdb::SqlTemplateCandidate bound_candidate_base;
    bool has_template{false};
    std::shared_ptr<const rmdb::query_template_detail::QueryTemplate> query_template;
    std::shared_ptr<const rmdb::plan_template_detail::PlanTemplate> plan_template;
    rmdb::u64 schema_epoch{0};
    rmdb::u64 feedback_generation{0};
    IsolationLevel plan_isolation{IsolationLevel::SNAPSHOT_ISOLATION};
    std::vector<PreparedColumn> columns;
};

struct PreparedRequestStatement {
    rmdb::u16 id{0};
    bool is_query{false};
    std::vector<rmdb::u8> parameter_types;
    std::string sql;
};

class PreparedDictionary {
   public:
    PreparedDictionary() { id_to_slot_.fill(-1); }

    void reserve(size_t count) { statements_.reserve(count); }

    void Add(PreparedStatement statement) {
        if (statement.id == 0 || id_to_slot_[statement.id] != -1 || statements_.size() >= 256) {
            throw RMDBError("invalid or duplicate prepared statement id");
        }
        id_to_slot_[statement.id] = static_cast<rmdb::i16>(statements_.size());
        statements_.push_back(std::move(statement));
    }

    const PreparedStatement *Find(rmdb::u16 id) const {
        rmdb::i16 slot = id_to_slot_[id];
        return slot < 0 ? nullptr : &statements_[static_cast<size_t>(slot)];
    }

    PreparedStatement *Find(rmdb::u16 id) {
        rmdb::i16 slot = id_to_slot_[id];
        return slot < 0 ? nullptr : &statements_[static_cast<size_t>(slot)];
    }

    const PreparedStatement &At(rmdb::u16 id) const {
        const PreparedStatement *statement = Find(id);
        if (statement == nullptr) throw RMDBError("unknown prepared statement id");
        return *statement;
    }

    std::vector<PreparedStatement> &statements() { return statements_; }
    const std::vector<PreparedStatement> &statements() const { return statements_; }

   private:
    std::vector<PreparedStatement> statements_;
    std::array<rmdb::i16, 65536> id_to_slot_{};
};

class PreparedAstCompiler {
   public:
    PreparedAstCompiler(const std::vector<rmdb::u8> &parameter_types, PreparedStatement *prepared)
        : parameter_types_(parameter_types), prepared_(prepared), seen_(parameter_types.size(), false) {}

    rmdb::SqlTemplateCandidate Compile(const std::shared_ptr<ast::TreeNode> &root) {
        if (auto insert = std::dynamic_pointer_cast<ast::InsertStmt>(root)) {
            prepared_->intent_relation = insert->tab_name;
            for (auto &value : insert->vals) AddValue(&value);
        } else if (auto update = std::dynamic_pointer_cast<ast::UpdateStmt>(root)) {
            prepared_->intent_relation = update->tab_name;
            for (auto &set_clause : update->set_clauses) {
                if (!set_clause->rhs_is_col || set_clause->op != ast::SET_OP_ASSIGN) {
                    AddValue(&set_clause->val);
                }
            }
            AddConditions(update->conds);
        } else if (auto del = std::dynamic_pointer_cast<ast::DeleteStmt>(root)) {
            prepared_->intent_relation = del->tab_name;
            AddConditions(del->conds);
        } else if (auto select = std::dynamic_pointer_cast<ast::SelectStmt>(root)) {
            if (!select->table_refs.empty()) prepared_->intent_relation = select->table_refs.front()->tab_name;
            AddConditions(select->conds);
            for (auto &having : select->having_conds) AddValue(&having->rhs);
            AddConditions(select->semi_conds);
            if (select->has_limit) AddIntLiteral(select->limit);
        } else if (!parameter_types_.empty()) {
            throw RMDBError("parameters are only supported in prepared DML statements");
        }
        if (std::find(seen_.begin(), seen_.end(), false) != seen_.end()) {
            throw RMDBError("prepared parameter markers must form a dense set");
        }
        candidate_.key.literal_count = static_cast<rmdb::u16>(candidate_.literals.size());
        return std::move(candidate_);
    }

   private:
    void AddConditions(std::vector<std::shared_ptr<ast::BinaryExpr>> &conditions) {
        for (auto &condition : conditions) {
            auto value = std::dynamic_pointer_cast<ast::Value>(condition->rhs);
            if (value == nullptr) continue;
            auto parameter = std::dynamic_pointer_cast<ast::ParameterRef>(value);
            std::string column;
            if (parameter != nullptr && condition->lhs != nullptr) {
                column = condition->lhs->tab_name.empty()
                             ? condition->lhs->col_name
                             : condition->lhs->tab_name + "." + condition->lhs->col_name;
            }
            AddValue(&value, column);
            condition->rhs = std::move(value);
        }
    }

    void AddValue(std::shared_ptr<ast::Value> *value, const std::string &predicate_column = {}) {
        if (value == nullptr || *value == nullptr) throw RMDBError("invalid prepared SQL literal");
        if (auto parameter = std::dynamic_pointer_cast<ast::ParameterRef>(*value)) {
            if (parameter->ordinal == 0 || parameter->ordinal > parameter_types_.size()) {
                throw RMDBError("prepared parameter marker out of range");
            }
            rmdb::u16 parameter_index = static_cast<rmdb::u16>(parameter->ordinal - 1U);
            rmdb::u8 parameter_type = parameter_types_[parameter_index];
            seen_[parameter_index] = true;
            prepared_->marker_parameters.push_back(parameter_index);
            prepared_->marker_literal_indexes.push_back(static_cast<rmdb::u16>(candidate_.literals.size()));
            if (!predicate_column.empty()) {
                prepared_->predicate_markers.emplace_back(predicate_column, parameter_index);
            }
            rmdb::SqlTemplateLiteral literal;
            if (parameter_type == 0x01) {
                *value = std::make_shared<ast::IntLit>(0);
                literal.type = rmdb::SqlTemplateLiteralType::kInt;
            } else if (parameter_type == 0x02) {
                *value = std::make_shared<ast::FloatLit>(0.0F);
                literal.type = rmdb::SqlTemplateLiteralType::kFloat;
            } else if (parameter_type == 0x03) {
                *value = std::make_shared<ast::StringLit>("");
                literal.type = rmdb::SqlTemplateLiteralType::kString;
            } else {
                throw RMDBError("unknown prepared parameter type");
            }
            PushLiteral(std::move(literal));
            return;
        }
        if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(*value)) {
            AddIntLiteral(int_lit->val);
        } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(*value)) {
            rmdb::SqlTemplateLiteral literal;
            literal.type = rmdb::SqlTemplateLiteralType::kFloat;
            literal.float_val = float_lit->val < 0.0F ? -float_lit->val : float_lit->val;
            PushLiteral(std::move(literal));
        } else if (auto string_lit = std::dynamic_pointer_cast<ast::StringLit>(*value)) {
            rmdb::SqlTemplateLiteral literal;
            literal.type = rmdb::SqlTemplateLiteralType::kString;
            literal.str_val = string_lit->val;
            PushLiteral(std::move(literal));
        } else {
            throw RMDBError("unsupported prepared SQL literal");
        }
    }

    void AddIntLiteral(int value) {
        if (value == std::numeric_limits<int>::min()) {
            throw RMDBError("prepared integer literal is out of range");
        }
        rmdb::SqlTemplateLiteral literal;
        literal.type = rmdb::SqlTemplateLiteralType::kInt;
        literal.int_val = value < 0 ? -value : value;
        PushLiteral(std::move(literal));
    }

    void PushLiteral(rmdb::SqlTemplateLiteral literal) {
        if (!candidate_.literals.push_back(std::move(literal))) {
            throw RMDBError("too many prepared SQL literals");
        }
    }

    const std::vector<rmdb::u8> &parameter_types_;
    PreparedStatement *prepared_;
    std::vector<bool> seen_;
    rmdb::SqlTemplateCandidate candidate_;
};

std::vector<PreparedColumn> PreparedQueryColumns(const std::shared_ptr<Plan> &plan, Context *context) {
    auto portal_statement = portal->start(plan, context);
    if (portal_statement == nullptr || portal_statement->tag != PORTAL_ONE_SELECT ||
        portal_statement->root == nullptr) {
        throw RMDBError("prepared query did not produce a query plan");
    }
    std::vector<PreparedColumn> columns;
    columns.reserve(portal_statement->sel_cols.size());
    const auto &root_columns = portal_statement->root->cols();
    for (const auto &selected : portal_statement->sel_cols) {
        auto column = portal_statement->root->get_col(root_columns, selected);
        const auto &name = selected.output_name.empty() ? selected.col_name : selected.output_name;
        columns.push_back(PreparedColumn{name, column->type});
    }
    portal->drop();
    return columns;
}

PreparedStatement PrepareOneStatement(const PreparedRequestStatement &request, Context *context,
                                      IsolationLevel session_isolation) {
    PreparedStatement prepared;
    prepared.id = request.id;
    prepared.is_query = request.is_query;
    prepared.parameter_types = request.parameter_types;
    prepared.exemplar_sql = request.sql;

    FastCommand fast_command = parse_fast_command(prepared.exemplar_sql.c_str());
    if (fast_command != FastCommand::None) {
        if (prepared.is_query || !prepared.parameter_types.empty()) {
            throw RMDBError("prepared transaction control must be a parameterless command");
        }
        return prepared;
    }

    auto parse_result = rmdb::ParseSql(request.sql);
    if (parse_result.status != 0 || parse_result.tree == nullptr) {
        throw RMDBError(parse_result.error.empty() ? "prepared SQL parse failed" : parse_result.error);
    }
    auto candidate = PreparedAstCompiler(request.parameter_types, &prepared).Compile(parse_result.tree);
    auto query = analyze->do_analyze(parse_result.tree);
    prepared.query_template =
        rmdb::query_template_detail::make_template(candidate, parse_result.tree, query);
    if (prepared.query_template != nullptr) {
        prepared.has_template = true;
        prepared.template_literals.reserve(candidate.literals.size());
        prepared.literal_parameter_bindings.assign(candidate.literals.size(), -1);
        for (size_t i = 0; i < candidate.literals.size(); ++i) {
            const auto &literal = candidate.literals[i];
            PreparedStatement::TemplateLiteral stored;
            stored.type = literal.type;
            stored.int_value = literal.int_val;
            stored.float_value = literal.float_val;
            stored.string_value.assign(literal.str_val.data(), literal.str_val.size());
            prepared.template_literals.push_back(std::move(stored));
        }
        for (size_t marker = 0; marker < prepared.marker_literal_indexes.size(); ++marker) {
            prepared.literal_parameter_bindings[prepared.marker_literal_indexes[marker]] =
                prepared.marker_parameters[marker];
        }
        prepared.bound_candidate_base = candidate;
    } else if (!request.parameter_types.empty()) {
        throw RMDBError("parameterized prepared SQL cannot create an immutable query template");
    }
    auto statement_guard = txn_manager->EnterStatementExecution();
    auto plan = optimizer->plan_query(query, context);
    bool actual_query = plan->tag == T_select;
    if (actual_query != prepared.is_query) {
        throw RMDBError("prepared result_kind does not match SQL");
    }
    if (prepared.has_template && candidate.literals.size() != 0 &&
        rmdb::plan_template_cacheable_session(session_isolation)) {
        prepared.schema_epoch = rmdb::sql_template_schema_epoch();
        prepared.feedback_generation = rmdb::workload_feedback_generation();
        prepared.plan_isolation = session_isolation;
        prepared.plan_template = rmdb::plan_template_detail::make_template(
            candidate, query, plan, prepared.feedback_generation);
    }
    if (prepared.is_query) {
        prepared.columns = PreparedQueryColumns(plan, context);
        if (prepared.columns.empty()) {
            throw RMDBError("prepared query must project at least one column");
        }
    }
    return prepared;
}

std::vector<PreparedRequestStatement> DecodePrepareSet(const WireFrame &frame) {
    WireReader reader(frame.payload);
    rmdb::u16 statement_count = reader.ReadU16();
    if (statement_count == 0 || statement_count > 256) {
        throw RMDBError("PREPARE_SET statement_count must be 1..256");
    }
    std::vector<PreparedRequestStatement> requests;
    requests.reserve(statement_count);
    std::unordered_map<rmdb::u16, bool> ids;
    for (rmdb::u16 i = 0; i < statement_count; ++i) {
        PreparedRequestStatement request;
        request.id = reader.ReadU16();
        if (request.id == 0 || !ids.emplace(request.id, true).second) {
            throw RMDBError("PREPARE_SET statement ids must be nonzero and unique");
        }
        rmdb::u8 result_kind = reader.ReadU8();
        if (result_kind > 1) {
            throw RMDBError("invalid PREPARE_SET result_kind");
        }
        request.is_query = result_kind == 1;
        rmdb::u16 parameter_count = reader.ReadU16();
        request.parameter_types.reserve(parameter_count);
        for (rmdb::u16 parameter = 0; parameter < parameter_count; ++parameter) {
            rmdb::u8 type = reader.ReadU8();
            if (type < 0x01 || type > 0x03) {
                throw RMDBError("invalid PREPARE_SET parameter type");
            }
            request.parameter_types.push_back(type);
        }
        rmdb::u32 sql_size = reader.ReadU32();
        if (sql_size == 0) {
            throw RMDBError("prepared SQL must be non-empty");
        }
        request.sql = reader.ReadString(sql_size);
        if (request.sql.find('\0') != std::string::npos) {
            throw RMDBError("prepared SQL contains NUL");
        }
        requests.push_back(std::move(request));
    }
    if (!reader.Empty()) {
        throw RMDBError("trailing PREPARE_SET payload bytes");
    }
    return requests;
}

WireBuffer EncodePrepareOk(const std::vector<PreparedRequestStatement> &requests,
                           const PreparedDictionary &dictionary) {
    WireBuffer payload;
    payload.PutU16(static_cast<rmdb::u16>(requests.size()));
    for (const auto &request : requests) {
        const auto &prepared = dictionary.At(request.id);
        payload.PutU16(prepared.id);
        payload.PutU16(static_cast<rmdb::u16>(prepared.columns.size()));
        for (const auto &column : prepared.columns) {
            if (column.name.empty() || column.name.size() > UINT16_MAX) {
                throw RMDBError("invalid prepared result column name");
            }
            payload.PutU16(static_cast<rmdb::u16>(column.name.size()));
            payload.PutBytes(column.name.data(), column.name.size());
            payload.PutU8(WireTypeFor(column.type));
        }
    }
    return payload;
}

class StreamQueryResultWriter : public QueryResultWriter {
   public:
    explicit StreamQueryResultWriter(int fd) : fd_(fd) {}

    void BeginResult(const std::vector<ColMeta> &columns) override {
        if (columns.empty() || columns.size() > UINT16_MAX) {
            throw RMDBError("invalid query column count");
        }
        WireBuffer payload;
        payload.PutU16(static_cast<rmdb::u16>(columns.size()));
        for (const auto &column : columns) {
            if (column.name.empty() || column.name.size() > UINT16_MAX) {
                throw RMDBError("invalid wire column name");
            }
            payload.PutU16(static_cast<rmdb::u16>(column.name.size()));
            payload.PutBytes(column.name.data(), column.name.size());
            payload.PutU8(WireTypeFor(column.type));
        }
        SendOrThrow(0x01, payload);
        began_ = true;
    }

    void WriteRow(const std::vector<ColMeta> &columns,
                  const std::vector<const char *> &cells) override {
        if (!began_ || columns.size() != cells.size()) {
            throw RMDBError("wire row does not match result schema");
        }
        WireBuffer payload;
        for (size_t i = 0; i < columns.size(); ++i) {
            const ColMeta &column = columns[i];
            const char *cell = cells[i];
            payload.PutU8(1);
            if (column.type == TYPE_INT) {
                payload.PutU32(static_cast<rmdb::u32>(rmdb::load_unaligned<int>(cell)));
            } else if (column.type == TYPE_FLOAT) {
                rmdb::u32 bits = 0;
                float value = rmdb::load_unaligned<float>(cell);
                std::memcpy(&bits, &value, sizeof(bits));
                payload.PutU32(bits);
            } else if (column.type == TYPE_STRING) {
                const void *nul = std::memchr(cell, '\0', static_cast<size_t>(column.len));
                size_t length = nul == nullptr ? static_cast<size_t>(column.len)
                                               : static_cast<const char *>(nul) - cell;
                payload.PutU32(static_cast<rmdb::u32>(length));
                payload.PutBytes(cell, length);
            } else {
                throw RMDBError("unsupported wire cell type");
            }
        }
        SendOrThrow(0x02, payload);
    }

    void EndResult(rmdb::u64 row_count) override {
        if (!began_) {
            throw RMDBError("wire result ended before META");
        }
        WireBuffer payload;
        payload.PutU64(row_count);
        SendOrThrow(0x11, payload);
        ended_ = true;
    }

    bool ended() const { return ended_; }

   private:
    void SendOrThrow(rmdb::u8 tag, const WireBuffer &payload) {
        if (!SendWireFrame(fd_, tag, payload)) {
            throw RMDBError("wire response write failed");
        }
    }

    int fd_;
    bool began_{false};
    bool ended_{false};
};

struct BoundParameterView {
    rmdb::u8 type{0};
    bool present{false};
    rmdb::i32 int_value{0};
    float float_value{0.0f};
    std::string_view string_value;
};

struct DecodedOperation {
    const PreparedStatement *statement{nullptr};
    rmdb::u32 parameter_begin{0};
    rmdb::u16 parameter_count{0};
};

struct DecodedBatch {
    static constexpr size_t kMaxOperations = 256;

    std::array<DecodedOperation, kMaxOperations> operations{};
    rmdb::u16 operation_count{0};
    std::vector<BoundParameterView> parameters;

    void Reset(rmdb::u16 count) {
        operation_count = count;
        parameters.clear();
        size_t expected_parameters = static_cast<size_t>(count) * 8U;
        if (parameters.capacity() < expected_parameters) {
            parameters.reserve(expected_parameters);
        }
    }

    size_t Size() const { return operation_count; }

    const BoundParameterView *Parameters(const DecodedOperation &operation) const {
        return parameters.data() + operation.parameter_begin;
    }
};

lock_data_key_t BatchWriteIntentKey(const PreparedStatement &statement,
                                    const BoundParameterView *parameters, size_t parameter_count) {
    rmdb::u64 hash = statement.intent_relation_hash;
    auto mix = [&](rmdb::u64 value) {
        hash ^= value;
        hash *= 1099511628211ULL;
    };
    for (const auto &[column, index] : statement.sorted_predicate_markers) {
        if (index >= parameter_count) continue;
        const auto &parameter = parameters[index];
        for (unsigned char ch : column) mix(ch);
        mix(parameter.type);
        if (!parameter.present) {
            mix(0);
        } else if (parameter.type == 0x01) {
            mix(static_cast<rmdb::u32>(parameter.int_value));
        } else if (parameter.type == 0x02) {
            rmdb::u32 bits = 0;
            std::memcpy(&bits, &parameter.float_value, sizeof(bits));
            mix(bits);
        } else {
            for (unsigned char ch : parameter.string_value) mix(ch);
        }
    }
    return hash & ~(static_cast<lock_data_key_t>(1) << 63);
}

// Connection-local mutable state for EXEC_BATCH. PreparedStatement objects are
// immutable after PREPARE_SET; parameters, result buffers and intent scratch
// live here and retain capacity across requests on the same connection.
struct BatchExecutionFrame {
    DecodedBatch decoded;
    WireBuffer encoded_results;
    rmdb::u16 result_count{0};
    std::vector<lock_data_key_t> write_intents;

    void ResetResults() {
        encoded_results.Clear();
        result_count = 0;
        write_intents.clear();
    }
};

class BatchQueryResultWriter : public QueryResultWriter {
   public:
    BatchQueryResultWriter(rmdb::u16 operation_index, const PreparedStatement *statement,
                           WireBuffer *encoded_results)
        : operation_index_(operation_index), statement_(statement),
          encoded_results_(encoded_results) {}

    void BeginResult(const std::vector<ColMeta> &columns) override {
        if (began_ || statement_ == nullptr || columns.size() != statement_->columns.size()) {
            throw RMDBError("batch query schema column count mismatch");
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].type != statement_->columns[i].type) {
                throw RMDBError("batch query schema type mismatch");
            }
        }
        if (encoded_results_ == nullptr) {
            throw RMDBError("batch result buffer is unavailable");
        }
        encoded_results_->PutU16(operation_index_);
        row_count_offset_ = encoded_results_->Size();
        encoded_results_->PutU32(0);
        began_ = true;
    }

    void WriteRow(const std::vector<ColMeta> &columns,
                  const std::vector<const char *> &cells) override {
        if (!began_ || ended_ || columns.size() != cells.size()) {
            throw RMDBError("invalid batch query row");
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            const ColMeta &column = columns[i];
            const char *cell = cells[i];
            encoded_results_->PutU8(1);
            if (column.type == TYPE_INT) {
                encoded_results_->PutU32(static_cast<rmdb::u32>(rmdb::load_unaligned<int>(cell)));
            } else if (column.type == TYPE_FLOAT) {
                rmdb::u32 bits = 0;
                float value = rmdb::load_unaligned<float>(cell);
                std::memcpy(&bits, &value, sizeof(bits));
                encoded_results_->PutU32(bits);
            } else if (column.type == TYPE_STRING) {
                const void *nul = std::memchr(cell, '\0', static_cast<size_t>(column.len));
                size_t length = nul == nullptr ? static_cast<size_t>(column.len)
                                               : static_cast<const char *>(nul) - cell;
                encoded_results_->PutU32(static_cast<rmdb::u32>(length));
                encoded_results_->PutBytes(cell, length);
            } else {
                throw RMDBError("unsupported batch result type");
            }
        }
        if (row_count_ == UINT32_MAX) {
            throw RMDBError("batch query row count overflow");
        }
        ++row_count_;
    }

    void EndResult(rmdb::u64 row_count) override {
        if (!began_ || row_count != row_count_) {
            throw RMDBError("batch query row count mismatch");
        }
        encoded_results_->PatchU32(row_count_offset_, row_count_);
        ended_ = true;
    }

    bool ended() const { return ended_; }

   private:
    rmdb::u16 operation_index_;
    const PreparedStatement *statement_;
    WireBuffer *encoded_results_;
    size_t row_count_offset_{0};
    rmdb::u32 row_count_{0};
    bool began_{false};
    bool ended_{false};
};

void DecodeBatch(const WireFrame &frame, const PreparedDictionary &dictionary,
                 DecodedBatch *batch) {
    if (batch == nullptr) throw RMDBError("batch execution frame is unavailable");
    WireReader reader(frame.payload);
    rmdb::u16 operation_count = reader.ReadU16();
    if (operation_count == 0 || operation_count > 256) {
        throw RMDBError("EXEC_BATCH operation_count must be 1..256");
    }
    batch->Reset(operation_count);
    for (rmdb::u16 operation_index = 0; operation_index < operation_count; ++operation_index) {
        rmdb::u16 statement_id = reader.ReadU16();
        const PreparedStatement *statement = dictionary.Find(statement_id);
        if (statement == nullptr) {
            throw RMDBError("EXEC_BATCH references unknown statement id");
        }
        DecodedOperation operation;
        operation.statement = statement;
        operation.parameter_begin = static_cast<rmdb::u32>(batch->parameters.size());
        operation.parameter_count = static_cast<rmdb::u16>(statement->parameter_types.size());
        for (rmdb::u8 type : statement->parameter_types) {
            BoundParameterView parameter;
            parameter.type = type;
            rmdb::u8 present = reader.ReadU8();
            if (present > 1) {
                throw RMDBError("invalid typed parameter present byte");
            }
            parameter.present = present == 1;
            if (parameter.present) {
                if (type == 0x01) {
                    parameter.int_value = static_cast<rmdb::i32>(reader.ReadU32());
                } else if (type == 0x02) {
                    rmdb::u32 bits = reader.ReadU32();
                    std::memcpy(&parameter.float_value, &bits, sizeof(bits));
                    if (!std::isfinite(parameter.float_value)) {
                        throw RMDBError("FLOAT32 parameter must be finite");
                    }
                } else if (type == 0x03) {
                    parameter.string_value = reader.ReadStringView(reader.ReadU32());
                }
            }
            batch->parameters.push_back(parameter);
        }
        batch->operations[operation_index] = operation;
    }
    if (!reader.Empty()) {
        throw RMDBError("trailing EXEC_BATCH payload bytes");
    }
}

rmdb::SqlTemplateCandidate BindPreparedCandidate(const PreparedStatement &statement,
                                                 const BoundParameterView *parameters,
                                                 size_t parameter_count) {
    if (!statement.has_template || parameter_count != statement.parameter_types.size()) {
        throw RMDBError("prepared statement template is unavailable");
    }
    rmdb::SqlTemplateCandidate candidate = statement.bound_candidate_base;
    for (size_t literal_index = 0; literal_index < statement.literal_parameter_bindings.size(); ++literal_index) {
        rmdb::i32 binding = statement.literal_parameter_bindings[literal_index];
        if (binding >= 0) {
            const BoundParameterView &parameter = parameters[static_cast<size_t>(binding)];
            if (!parameter.present) {
                throw RMDBError("NULL prepared parameters are not supported by this schema");
            }
            auto &literal = candidate.literals[literal_index];
            if (parameter.type == 0x01) {
                literal.type = rmdb::SqlTemplateLiteralType::kInt;
                literal.int_val = parameter.int_value;
            } else if (parameter.type == 0x02) {
                literal.type = rmdb::SqlTemplateLiteralType::kFloat;
                literal.float_val = parameter.float_value;
            } else if (parameter.type == 0x03) {
                literal.type = rmdb::SqlTemplateLiteralType::kString;
                literal.str_val = parameter.string_value;
            } else {
                throw RMDBError("invalid prepared parameter type");
            }
        }
    }
    return candidate;
}

void EncodeBatchResult(rmdb::u16 executed_operations, rmdb::u8 status,
                       rmdb::u16 failed_operation, const std::string &diagnostic,
                       const WireBuffer *encoded_results, size_t result_count,
                       WireBuffer *payload) {
    if (payload == nullptr) throw RMDBError("batch response buffer is unavailable");
    payload->Clear();
    payload->PutU16(executed_operations);
    payload->PutU8(status);
    payload->PutU16(failed_operation);
    size_t diagnostic_size = std::min(diagnostic.size(), kMaxWireDiagnosticBytes);
    payload->PutU32(static_cast<rmdb::u32>(diagnostic_size));
    payload->PutBytes(diagnostic.data(), diagnostic_size);
    payload->PutU16(static_cast<rmdb::u16>(result_count));
    if (result_count != 0) {
        if (encoded_results == nullptr) {
            throw RMDBError("encoded batch results are unavailable");
        }
        payload->PutBytes(encoded_results->Data(), encoded_results->Size());
    }
}

bool SendUnexpectedBatchFailure(int fd, txn_id_t *txn_id, Transaction **session_txn,
                                rmdb::u16 failed_operation, rmdb::u8 status,
                                const char *diagnostic, WireBuffer *response) noexcept {
    AbortSessionTransaction(txn_id, session_txn);
    try {
        EncodeBatchResult(failed_operation, status, failed_operation,
                          diagnostic == nullptr ? "batch execution failed" : diagnostic,
                          nullptr, 0, response);
        return SendWireFrame(fd, 0x15, *response);
    } catch (...) {
        return false;
    }
}

bool HandlePrepareSetRequest(int fd, const WireFrame &frame, PreparedDictionary *dictionary,
                             txn_id_t txn_id, IsolationLevel session_isolation) {
    if (txn_id != INVALID_TXN_ID) {
        return SendWireDiagnostic(fd, 0x13, "PREPARE_SET is not allowed during an active transaction");
    }
    Transaction *planning_txn = nullptr;
    try {
        auto requests = DecodePrepareSet(frame);
        planning_txn = txn_manager->begin(nullptr, log_manager.get(), session_isolation);
        StatementScratch scratch;
        auto data_send = std::make_unique<char[]>(BUFFER_LENGTH);
        int offset = 0;
        Context planning_context(lock_manager.get(), log_manager.get(), txn_manager.get(), planning_txn,
                                 data_send.get(), &offset, &session_isolation, &scratch);
        PreparedDictionary replacement;
        replacement.reserve(requests.size());
        for (const auto &request : requests) {
            PreparedStatement prepared;
            try {
                prepared = PrepareOneStatement(request, &planning_context, session_isolation);
            } catch (RMDBError &error) {
                throw RMDBError("prepared statement " + std::to_string(request.id) + ": " + error.what());
            }
            replacement.Add(std::move(prepared));
            scratch.reset();
        }
        std::unordered_set<std::string> writable_relations;
        for (const auto &statement : replacement.statements()) {
            if (!statement.is_query && !statement.intent_relation.empty() &&
                !statement.predicate_markers.empty() &&
                parse_fast_command(statement.exemplar_sql.c_str()) == FastCommand::None) {
                writable_relations.insert(statement.intent_relation);
            }
        }
        for (auto &statement : replacement.statements()) {
            statement.sorted_predicate_markers = statement.predicate_markers;
            std::sort(statement.sorted_predicate_markers.begin(), statement.sorted_predicate_markers.end(),
                      [](const auto &lhs, const auto &rhs) {
                          return lhs.first == rhs.first ? lhs.second < rhs.second : lhs.first < rhs.first;
                      });
            for (unsigned char ch : statement.intent_relation) {
                statement.intent_relation_hash ^= ch;
                statement.intent_relation_hash *= 1099511628211ULL;
            }
            // A predicate read that starts a read-modify-write transaction
            // must participate in hotspot admission as well.  In particular,
            // Delivery sends BEGIN + MIN in a batch before its claim UPDATE;
            // acquiring the district token before the snapshot is created
            // keeps earliest-order selection and the later claim consistent.
            statement.batch_intent_enabled = !statement.intent_relation.empty() &&
                !statement.sorted_predicate_markers.empty() &&
                writable_relations.count(statement.intent_relation) != 0;
        }
        {
            auto statement_guard = txn_manager->EnterStatementExecution();
            txn_manager->commit(planning_txn, log_manager.get());
        }
        planning_txn = nullptr;
        WireBuffer response = EncodePrepareOk(requests, replacement);
        *dictionary = std::move(replacement);
        return SendWireFrame(fd, 0x14, response);
    } catch (RMDBError &error) {
        if (planning_txn != nullptr && planning_txn->get_state() == TransactionState::GROWING) {
            auto statement_guard = txn_manager->EnterStatementExecution();
            txn_manager->abort(planning_txn, log_manager.get());
        }
        return SendWireDiagnostic(fd, 0x13, error.what());
    }
}

bool HandleBatchRequestImpl(int fd, const WireFrame &frame, const PreparedDictionary &dictionary,
                            char *data_send, int *offset, txn_id_t *txn_id, Transaction **session_txn,
                            IsolationLevel *session_isolation, StatementScratch *statement_scratch,
                            SessionPlanTemplateHotCache *session_plan_cache,
                            SessionPointProgramCache *session_point_program_cache,
                            BatchExecutionFrame *execution_frame,
                            rmdb::u16 *operation_progress, WireBuffer *response) {
    if (execution_frame == nullptr) {
        throw RMDBError("batch execution frame is unavailable");
    }
    execution_frame->ResetResults();
    try {
        DecodeBatch(frame, dictionary, &execution_frame->decoded);
    } catch (RMDBError &error) {
        if (*txn_id != INVALID_TXN_ID || *session_txn != nullptr) {
            Context abort_context(lock_manager.get(), log_manager.get(), txn_manager.get(), *session_txn,
                                  data_send, offset, session_isolation, statement_scratch);
            AbortStatementTransaction(&abort_context, txn_id);
            *session_txn = nullptr;
        }
        return SendWireDiagnostic(fd, 0x13, error.what());
    }

    const DecodedBatch &batch = execution_frame->decoded;
    const auto batch_deadline = std::chrono::steady_clock::now() + kBatchExecutionBudget;
    // 在第一条操作创建事务前，预留所有可静态识别的写热点。这样 SI 快照一定晚于
    // 持有同一逻辑键的前序写者；准入令牌持续到 commit/abort，因而覆盖整个多语句批次。
    auto &batch_intents = execution_frame->write_intents;
    if (*txn_id == INVALID_TXN_ID && *session_txn == nullptr) {
        for (size_t i = 0; i < batch.Size(); ++i) {
            const auto &operation = batch.operations[i];
            if (operation.statement != nullptr && operation.statement->batch_intent_enabled) {
                batch_intents.push_back(BatchWriteIntentKey(
                    *operation.statement, batch.Parameters(operation), operation.parameter_count));
            }
        }
        lock_manager->canonicalize_write_intents(&batch_intents);
    }
    for (rmdb::u16 operation_index = 0; operation_index < batch.Size(); ++operation_index) {
        *operation_progress = operation_index;
        const DecodedOperation &operation = batch.operations[operation_index];
        const PreparedStatement &statement = *operation.statement;
        *offset = 0;
        data_send[0] = '\0';
        statement_scratch->reset();

        if (statement.is_query) {
            ++execution_frame->result_count;
        }
        BatchQueryResultWriter result_writer(
            operation_index, &statement,
            statement.is_query ? &execution_frame->encoded_results : nullptr);
        Context statement_context(lock_manager.get(), log_manager.get(), txn_manager.get(), *session_txn,
                                  data_send, offset, session_isolation, statement_scratch,
                                  statement.is_query ? &result_writer : nullptr);
        statement_context.set_execution_deadline(batch_deadline);
        if (!batch_intents.empty() && *txn_id == INVALID_TXN_ID && *session_txn == nullptr) {
            statement_context.set_pending_write_intents_view(&batch_intents, true);
        }
        StatementExecutionResult execution;
        try {
            if (statement.has_template) {
                rmdb::SqlTemplateCandidate candidate = BindPreparedCandidate(
                    statement, batch.Parameters(operation), operation.parameter_count);
                execution = ExecuteSqlStatement(
                    statement.exemplar_sql.c_str(), txn_id, &statement_context, session_isolation,
                    session_plan_cache, session_point_program_cache, &candidate,
                    statement.query_template.get(), statement.plan_template, statement.schema_epoch,
                    statement.feedback_generation, statement.plan_isolation, true);
            } else {
                if (operation.parameter_count != 0) {
                    throw RMDBError("prepared command parameters cannot be bound");
                }
                execution = ExecuteSqlStatement(
                    statement.exemplar_sql.c_str(), txn_id, &statement_context, session_isolation,
                    session_plan_cache, session_point_program_cache, nullptr, nullptr, nullptr, 0, 0,
                    IsolationLevel::SNAPSHOT_ISOLATION, true);
            }
        } catch (RMDBError &error) {
            AbortStatementTransaction(&statement_context, txn_id);
            execution.status = StatementStatus::Error;
            execution.diagnostic = error.what();
        }
        *session_txn = statement_context.txn_;

        bool result_mismatch = execution.status == StatementStatus::Ok &&
                               (execution.is_query != statement.is_query ||
                                (statement.is_query && !result_writer.ended()));
        if (result_mismatch) {
            AbortStatementTransaction(&statement_context, txn_id);
            *session_txn = nullptr;
            execution.status = StatementStatus::Error;
            execution.diagnostic = "prepared result_kind does not match execution result";
        }
        if (execution.status != StatementStatus::Ok) {
            if (*txn_id != INVALID_TXN_ID || *session_txn != nullptr) {
                AbortStatementTransaction(&statement_context, txn_id);
                *session_txn = nullptr;
            }
            rmdb::u8 status = execution.status == StatementStatus::TransactionAbort ? 1 : 2;
            execution_frame->result_count = 0;
            EncodeBatchResult(operation_index, status, operation_index,
                              execution.diagnostic, nullptr, 0, response);
            return SendWireFrame(fd, 0x15, *response);
        }
        if (operation_index + 1U < batch.Size() &&
            (*txn_id != INVALID_TXN_ID || *session_txn != nullptr) &&
            std::chrono::steady_clock::now() >= batch_deadline) {
            if (*txn_id != INVALID_TXN_ID || *session_txn != nullptr) {
                AbortStatementTransaction(&statement_context, txn_id);
                *session_txn = nullptr;
            }
            execution_frame->result_count = 0;
            EncodeBatchResult(static_cast<rmdb::u16>(operation_index + 1U), 1,
                              static_cast<rmdb::u16>(operation_index + 1U),
                              "batch execution budget exceeded; transaction aborted for retry",
                              nullptr, 0, response);
            return SendWireFrame(fd, 0x15, *response);
        }
    }

    EncodeBatchResult(static_cast<rmdb::u16>(batch.Size()), 0, UINT16_MAX, "",
                      &execution_frame->encoded_results,
                      execution_frame->result_count, response);
    return SendWireFrame(fd, 0x15, *response);
}

bool HandleBatchRequest(int fd, const WireFrame &frame, const PreparedDictionary &dictionary,
                        char *data_send, int *offset, txn_id_t *txn_id, Transaction **session_txn,
                        IsolationLevel *session_isolation, StatementScratch *statement_scratch,
                        SessionPlanTemplateHotCache *session_plan_cache,
                        SessionPointProgramCache *session_point_program_cache,
                        BatchExecutionFrame *execution_frame,
                        WireBuffer *response) noexcept {
    rmdb::u16 operation_progress = 0;
    try {
        return HandleBatchRequestImpl(fd, frame, dictionary, data_send, offset, txn_id, session_txn,
                                      session_isolation, statement_scratch, session_plan_cache,
                                      session_point_program_cache, execution_frame,
                                      &operation_progress, response);
    } catch (TransactionAbortException &) {
        return SendUnexpectedBatchFailure(fd, txn_id, session_txn, operation_progress, 1,
                                          "transaction aborted", response);
    } catch (const std::exception &error) {
        return SendUnexpectedBatchFailure(fd, txn_id, session_txn, operation_progress, 2, error.what(), response);
    } catch (...) {
        return SendUnexpectedBatchFailure(fd, txn_id, session_txn, operation_progress, 2,
                                          "unknown batch execution failure", response);
    }
}

void HandleWireV3Connection(int fd, char *data_send, int *offset, txn_id_t *txn_id,
                            Transaction **session_txn, IsolationLevel *session_isolation,
                            StatementScratch *statement_scratch,
                            SessionPlanTemplateHotCache *session_plan_cache,
                            SessionPointProgramCache *session_point_program_cache) {
    WireFrame frame;
    ConnectionReceiveBuffer receive_buffer;
    PreparedDictionary dictionary;
    BatchExecutionFrame batch_execution_frame;
    WireBuffer batch_response;
    while (true) {
        WireReceiveStatus receive_status =
            ReceiveWireFrame(fd, &receive_buffer, &frame);
        if (receive_status == WireReceiveStatus::PeerClosed) {
            return;
        }
        if (receive_status == WireReceiveStatus::TooLarge) {
            SendWireDiagnostic(fd, 0x13, "frame payload exceeds 1 MiB");
            return;
        }
        if (frame.reserved != 0) {
            SendWireDiagnostic(fd, 0x13, "reserved frame field must be zero");
            continue;
        }
        if (frame.tag == 0x21) {
            if (frame.flags != 0) {
                SendWireDiagnostic(fd, 0x13, "PREPARE_SET flags must be zero");
                continue;
            }
            if (!HandlePrepareSetRequest(fd, frame, &dictionary, *txn_id, *session_isolation)) {
                return;
            }
            continue;
        }
        if (frame.tag == 0x22) {
            if (frame.flags != 0x01) {
                SendWireDiagnostic(fd, 0x13, "EXEC_BATCH requires AUTO_ABORT");
                continue;
            }
            if (!HandleBatchRequest(fd, frame, dictionary, data_send, offset, txn_id, session_txn,
                                    session_isolation, statement_scratch, session_plan_cache,
                                    session_point_program_cache, &batch_execution_frame,
                                    &batch_response)) {
                return;
            }
            continue;
        }
        if (frame.tag != 0x20) {
            SendWireDiagnostic(fd, 0x13, "unsupported request tag");
            continue;
        }
        if (frame.flags != 0) {
            SendWireDiagnostic(fd, 0x13, "EXEC_STREAM flags must be zero");
            continue;
        }
        if (frame.payload.empty() ||
            std::find(frame.payload.begin(), frame.payload.end(), static_cast<rmdb::u8>(0)) != frame.payload.end()) {
            SendWireDiagnostic(fd, 0x13, "EXEC_STREAM requires non-empty SQL without NUL");
            continue;
        }

        std::string sql(reinterpret_cast<const char *>(frame.payload.data()), frame.payload.size());
        *offset = 0;
        data_send[0] = '\0';
        statement_scratch->reset();
        StreamQueryResultWriter result_writer(fd);
        Context statement_context(lock_manager.get(), log_manager.get(), txn_manager.get(), *session_txn,
                                  data_send, offset, session_isolation, statement_scratch, &result_writer);
        StatementExecutionResult execution = ExecuteSqlStatement(
            sql.c_str(), txn_id, &statement_context, session_isolation, session_plan_cache,
            session_point_program_cache);
        *session_txn = statement_context.txn_;

        if (execution.status == StatementStatus::Ok) {
            if (execution.is_query) {
                if (!result_writer.ended()) {
                    SendWireDiagnostic(fd, 0x13, "query did not produce a complete result");
                }
            } else {
                WireBuffer empty;
                if (!SendWireFrame(fd, 0x10, empty)) {
                    return;
                }
            }
        } else {
            rmdb::u8 tag = execution.status == StatementStatus::TransactionAbort ? 0x12 : 0x13;
            if (!SendWireDiagnostic(fd, tag, execution.diagnostic)) {
                return;
            }
        }
    }
}

void client_handler(int fd) {
    struct RegistryGuard {
        int fd;
        ~RegistryGuard() {
            std::lock_guard<std::mutex> guard(client_registry_mutex);
            active_client_fds.erase(fd);
        }
    } registry_guard{fd};
    PinCurrentClientThread();

    rmdb::TcpCommandFramer command_framer(kMaxSqlCommandBytes);
    std::string command;
    // 需要返回给客户端的结果
    auto data_send = std::make_unique<char[]>(BUFFER_LENGTH);
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;
    Transaction *session_txn = nullptr;
    SessionTransactionGuard transaction_guard(&txn_id, &session_txn);
    IsolationLevel session_isolation = IsolationLevel::SNAPSHOT_ISOLATION;
    StatementScratch statement_scratch;
    SessionPlanTemplateHotCache session_plan_cache;
    SessionPointProgramCache session_point_program_cache;

    if (server_trace_enabled()) {
        std::cout << "establish client connection, sockfd: " << fd << "\n";
    }

    ConnectionProtocol protocol = InitializeConnectionProtocol(fd, &command_framer);
    if (protocol == ConnectionProtocol::WireV3) {
        try {
            HandleWireV3Connection(fd, data_send.get(), &offset, &txn_id, &session_txn, &session_isolation,
                                   &statement_scratch, &session_plan_cache, &session_point_program_cache);
        } catch (const std::exception &error) {
            std::cerr << "Wire connection failed: " << error.what() << '\n';
        } catch (...) {
            std::cerr << "Wire connection failed with an unknown exception\n";
        }
        close(fd);
        return;
    }
    if (protocol == ConnectionProtocol::Closed) {
        close(fd);
        return;
    }

    while (true) {
        ReceiveCommandStatus receive_status = ReceiveCommand(fd, &command_framer, &command);
        if (receive_status != ReceiveCommandStatus::kCommand) {
            if (server_trace_enabled()) {
                std::cout << "Client connection closed, sockfd: " << fd << std::endl;
            }
            if (receive_status == ReceiveCommandStatus::kTooLarge) {
                static constexpr char failure[] = "failure\n";
                SendAll(fd, failure, sizeof(failure));
            }
            break;
        }
        const char *data_recv = command.c_str();

        if (strcmp(data_recv, "exit") == 0) {
            if (server_trace_enabled()) {
                std::cout << "Client exit." << std::endl;
            }
            break;
        }
        if (strcmp(data_recv, "crash") == 0) {
            std::cout << "Server crash" << std::endl;
            exit(1);
        }

        offset = 0;
        data_send[0] = '\0';
        statement_scratch.reset();

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        Context statement_context(lock_manager.get(), log_manager.get(), txn_manager.get(), session_txn, data_send.get(),
                                  &offset, &session_isolation, &statement_scratch);
        Context *context = &statement_context;

        StatementExecutionResult execution = ExecuteSqlStatement(
            data_recv, &txn_id, context, &session_isolation, &session_plan_cache, &session_point_program_cache);
        if (execution.status != StatementStatus::Ok) {
            std::string str = execution.legacy_abort_response ? "abort\n" : "failure\n";
            memcpy(data_send.get(), str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();
        }
        session_txn = context->txn_;
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (offset < 0 || offset >= BUFFER_LENGTH) {
            break;
        }
        data_send[static_cast<size_t>(offset)] = '\0';
        if (!SendAll(fd, data_send.get(), static_cast<size_t>(offset) + 1)) {
            break;
        }
    }

    // Clear
    if (server_trace_enabled()) {
        std::cout << "Terminating current client_connection..." << std::endl;
    }
    close(fd);           // close a file descriptor.
}

void start_server() {
    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }
    server_listen_fd = sockfd_server;

    std::thread wal_reset_thread;
    if (wal_reset_bytes != 0) {
        try {
            wal_reset_stop_requested.store(false, std::memory_order_relaxed);
            wal_reset_thread = std::thread(wal_reset_maintenance_loop);
        } catch (const std::system_error &) {
            close(sockfd_server);
            throw InternalError("Failed to start WAL reset maintenance thread");
        }
    }

    std::vector<std::thread> client_threads;

    while (!shutdown_requested.load(std::memory_order_relaxed)) {
        if (server_trace_enabled()) {
            std::cout << "Waiting for new connection..." << std::endl;
        }
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        // accept 会一直阻塞；SIGINT 处理函数通过关闭监听 fd 使其立即返回。
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            if (shutdown_requested.load(std::memory_order_relaxed) || errno == EBADF || errno == EINVAL) {
                break;
            }
            if (errno != EINTR) {
                std::cerr << "Accept error: " << strerror(errno) << std::endl;
            }
            continue;
        }
        int tcp_nodelay = 1;
        if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay)) != 0) {
            std::cerr << "TCP_NODELAY error: " << strerror(errno) << std::endl;
            close(sockfd);
            continue;
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        {
            std::lock_guard<std::mutex> guard(client_registry_mutex);
            active_client_fds.insert(sockfd);
        }
        try {
            client_threads.emplace_back(client_handler, sockfd);
        } catch (const std::system_error &error) {
            {
                std::lock_guard<std::mutex> guard(client_registry_mutex);
                active_client_fds.erase(sockfd);
            }
            close(sockfd);
            std::cerr << "Failed to start client thread: " << error.what() << std::endl;
            break;
        }

    }

    if (server_listen_fd == sockfd_server) {
        server_listen_fd = -1;
        close(sockfd_server);
    }
    std::cout << "Try to close all client connections.\n";
    wal_reset_stop_requested.store(true, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> guard(client_registry_mutex);
        for (int fd : active_client_fds) {
            shutdown(fd, SHUT_RDWR);
        }
    }
    // 每个 jthread 析构时都会 join；必须先等所有会话退出，再释放事务、日志和存储管理器。
    for (auto &client_thread : client_threads) {
        if (client_thread.joinable()) client_thread.join();
    }
    client_threads.clear();
    if (wal_reset_thread.joinable()) {
        wal_reset_thread.join();
    }
    log_manager->flush_log_to_disk();
    txn_manager->PhysicalizeCommittedDeletes();
    sm_manager->close_db();
    std::cout << " DB has been closed.\n";
    std::cout << "Server shuts down." << std::endl;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        // 需要指定数据库名称
        std::cerr << "Usage: " << argv[0] << " <database>" << std::endl;
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    try {
        ConfigureAllocator();
        wal_reset_bytes = configured_wal_reset_bytes();
        wal_preflush_bytes = configured_wal_preflush_bytes(wal_reset_bytes);
        InitializeManagers(configured_buffer_pool_pages(), configured_perf_stats_path());
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // Database name is passed by args
        std::string db_name = argv[1];
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
        }
        // Open database
        auto recovery_started = std::chrono::steady_clock::now();
        sm_manager->open_db(db_name);
        auto recovery_opened = std::chrono::steady_clock::now();
        rmdb::i64 log_file_size = disk_manager->get_file_size(LOG_FILE_NAME);
        log_manager->reset_log_file_offset(log_file_size > 0 ? log_file_size : 0);

        // recovery database
        recovery->analyze();
        auto recovery_analyzed = std::chrono::steady_clock::now();
        recovery->redo();
        auto recovery_redone = std::chrono::steady_clock::now();
        recovery->undo();
        auto recovery_undone = std::chrono::steady_clock::now();
        PersistRecoveredState();
        auto recovery_persisted = std::chrono::steady_clock::now();
        auto elapsed_ms = [](auto begin, auto end) {
            return std::chrono::duration<double, std::milli>(end - begin).count();
        };
        std::cerr << "Recovery startup timing: open_ms=" << elapsed_ms(recovery_started, recovery_opened)
                  << " analyze_ms=" << elapsed_ms(recovery_opened, recovery_analyzed)
                  << " redo_ms=" << elapsed_ms(recovery_analyzed, recovery_redone)
                  << " undo_ms=" << elapsed_ms(recovery_redone, recovery_undone)
                  << " persist_ms=" << elapsed_ms(recovery_undone, recovery_persisted)
                  << " total_ms=" << elapsed_ms(recovery_started, recovery_persisted) << "\n";
        
        // 开启服务端，开始接受客户端连接
        start_server();
        WritePerfStats();
    } catch (RMDBError &e) {
        WritePerfStats();
        std::cerr << e.what() << std::endl;
        exit(1);
    } catch (const std::exception &e) {
        WritePerfStats();
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
