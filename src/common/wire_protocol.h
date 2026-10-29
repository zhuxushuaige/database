/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

/**
 * SQL scalar tags used by RMDB Wire Protocol v3.
 * 协议只传输类型化值，不使用文本格式代替数值编码。
 */
enum class WireSqlType : uint8_t {
    INT32 = 0x01,
    FLOAT32 = 0x02,
    CHAR = 0x03,
};

/** Query-column metadata emitted once before streamed rows or in PREPARE_OK. */
struct WireColumn {
    std::string name;
    WireSqlType type = WireSqlType::CHAR;
};

/**
 * A typed protocol cell. FLOAT32 keeps its in-memory binary32 value and CHAR
 * stores logical bytes without page padding or a trailing NUL.
 */
struct WireCell {
    WireSqlType type = WireSqlType::CHAR;
    bool present = true;
    int32_t int_value = 0;
    float float_value = 0;
    std::string string_value;

    static WireCell Int32(int32_t value) {
        WireCell cell;
        cell.type = WireSqlType::INT32;
        cell.int_value = value;
        return cell;
    }

    static WireCell Float32(float value) {
        WireCell cell;
        cell.type = WireSqlType::FLOAT32;
        cell.float_value = value;
        return cell;
    }

    static WireCell Char(std::string value) {
        WireCell cell;
        cell.type = WireSqlType::CHAR;
        cell.string_value = std::move(value);
        return cell;
    }

    static WireCell FromText(WireSqlType type, const std::string &value) {
        switch (type) {
            case WireSqlType::INT32:
                return Int32(static_cast<int32_t>(std::stol(value)));
            case WireSqlType::FLOAT32:
                return Float32(std::stof(value));
            case WireSqlType::CHAR:
                return Char(value);
        }
        return Char(value);
    }
};

/** Connection-local result staging used by bounded query responses. */
struct WireQueryResult {
    bool is_query = false;
    bool header_pending = false;
    std::vector<WireColumn> columns;
    std::vector<std::vector<WireCell>> rows;

    void Clear() {
        is_query = false;
        header_pending = false;
        columns.clear();
        rows.clear();
    }
};
