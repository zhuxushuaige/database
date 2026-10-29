/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#include <limits>
#include <utility>

#include "parser.h"
#include "parser_defs.h"

namespace rmdb {

namespace {

class ScannerResource {
   public:
    ScannerResource() : initialized_(yylex_init(&scanner_) == 0) {}
    ~ScannerResource() {
        if (initialized_) yylex_destroy(scanner_);
    }

    ScannerResource(const ScannerResource &) = delete;
    ScannerResource &operator=(const ScannerResource &) = delete;

    explicit operator bool() const { return initialized_; }
    yyscan_t get() const { return scanner_; }

   private:
    yyscan_t scanner_{nullptr};
    bool initialized_{false};
};

class ScannerBufferResource {
   public:
    ScannerBufferResource(std::string_view sql, yyscan_t scanner)
        : scanner_(scanner), buffer_(yy_scan_bytes(sql.data(), static_cast<int>(sql.size()), scanner)) {}
    ~ScannerBufferResource() {
        if (buffer_ != nullptr) yy_delete_buffer(buffer_, scanner_);
    }

    ScannerBufferResource(const ScannerBufferResource &) = delete;
    ScannerBufferResource &operator=(const ScannerBufferResource &) = delete;

    explicit operator bool() const { return buffer_ != nullptr; }

   private:
    yyscan_t scanner_;
    YY_BUFFER_STATE buffer_{nullptr};
};

}  // namespace

ParseResult ParseSql(std::string_view sql) {
    ParseResult result;
    if (sql.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        result.error = "SQL input exceeds parser length limit";
        return result;
    }

    ScannerResource scanner;
    if (!scanner) {
        result.error = "Failed to initialize SQL scanner";
        return result;
    }

    ScannerBufferResource buffer(sql, scanner.get());
    if (!buffer) {
        result.error = "Failed to allocate SQL scanner buffer";
        return result;
    }

    ParserContext context;
    result.status = yyparse(&context, scanner.get());
    result.tree = std::move(context.result);
    result.error = std::move(context.error);
    return result;
}

}  // namespace rmdb
